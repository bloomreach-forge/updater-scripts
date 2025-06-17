package org.bloomreach.cms.scripts

import org.apache.commons.lang3.StringUtils
import org.onehippo.repository.update.BaseNodeUpdateVisitor

import javax.jcr.*
import java.util.function.Function
import java.util.stream.Collectors
import java.util.stream.Stream

/**
 * Convert existing landing pages (Prototype Pages) to Experience Pages
 * Warning: If a suitable prototype page is not found, the script will skip the conversion for that hst:page.
 *
 * XPath:  hst:{projectId}/hst:configurations
 *
 * Parameters:
 *   hstConfigurations (String)     = /hst:{projectId}/hst:configurations/{hstConfiguration} comma separated list of HST configurations to process
 *   xPageFolder (String)           = Root folder for XPages, default is "xpages"
 *   defaultXPageDocType (String)   = Default document type for XPages, default is "xpagemixin"
 *   parseInheritedHstConfiguration = Whether to parse inherited HST configuration, default is false
 * Example: {"hstConfigurations": "bloomreach, en-au, en-ca, en-de, en-gb, en-nz", "defaultXpageDocType":"bloomreach:SEOExperiencePage", "xPageFolder" : "xpages", "parseInheritedHstConfiguration": true}
 */
class PrototypeToExperiencePageConfigurationConverter extends BaseNodeUpdateVisitor {

    String hstConfigurations
    String xPageFolder
    String defaultXpageDocType
    Boolean parseInheritedHstConfiguration

    // Initializing parameters from the script
    void initialize(Session session) {
        hstConfigurations = (String) parametersMap.get("hstConfigurations")
        xPageFolder = (String) parametersMap.getOrDefault("xPageFolder", "xpages")
        defaultXpageDocType = (String) parametersMap.get("defaultXpageDocType")
        parseInheritedHstConfiguration = (Boolean) parametersMap.getOrDefault("parseInheritedHstConfiguration", false)
        if (StringUtils.isEmpty(hstConfigurations)) {
            log.error("hstConfigurations property is empty")
            throw new RuntimeException("hstConfigurations property is empty")
        }
        if (StringUtils.isEmpty(defaultXpageDocType)) {
            log.error("defaultXpageDocType property is empty")
            throw new RuntimeException("defaultXpageDocType property is empty")
        }
        log.info("Starting the LandingPage2XPageConversion script with configuration:" +
                "\nhstConfiguration=" + hstConfigurations +
                "\nxPageFolder=" + xPageFolder +
                "\ndefaultXpageDocType=" + defaultXpageDocType +
                "\nparseInheritedHstConfiguration=" + parseInheritedHstConfiguration);
    }

    @Override
    boolean doUpdate(final Node node) throws RepositoryException {
        if (node.isNodeType("hst:configurations")) {
            // Parse hstConfigurations into a list of trimmed configuration names
            List<String> configList = hstConfigurations.split(",").collect { it.trim() }
            log.info("Processing HST configurations: " + configList)

            // Process each configuration in the list
            configList.forEach { hstConfiguration ->
                log.info("Processing configuration: " + hstConfiguration)
                if (node.hasNode(hstConfiguration)) {
                    final NodeIterator it = node.getNodes(hstConfiguration)
                    while (it.hasNext()) {
                        final Node configuration = it.nextNode()
                        if (configuration.isNodeType("hst:configuration")) {
                            log.info("Initializing the in-memory channel configurations model")
                            HstConfiguration hstConfig = new HstConfiguration(configuration)
                            log.debug("Full hstConfig to be processed: \nprotoTypePages={}\nworkspace={}\nsiteMap={}\nxPages={}\nname={}\npath={}\nhst={}\nhstConfigNode={}\npages={}\nsession={}",
                                    hstConfig.protoTypePages,
                                    hstConfig.workspace,
                                    hstConfig.siteMap,
                                    hstConfig.xPages,
                                    hstConfig.name,
                                    hstConfig.path,
                                    hstConfig.hst,
                                    hstConfig.hstConfigNode,
                                    hstConfig.pages,
                                    hstConfig.session
                            );
                            hstConfig.startConversion()
                        }
                    }
                }
            }
        }
        return true
    }

    // Undoing updates is not supported in this script
    @Override
    boolean undoUpdate(final Node node) throws RepositoryException, UnsupportedOperationException {
        return false
    }

    interface ToNode {
        void toNode(Node container) throws RepositoryException
    }

    private class HstConfiguration {
        static ProtoTypePages protoTypePages
        final Workspace workspace
        final SiteMap siteMap
        final XPages xPages
        final String name
        final String path
        final Node hst
        final Node hstConfigNode
        final Pages pages
        final Session session

        HstConfiguration(Node hstConfiguration) throws RepositoryException {
            // Define variables based on either the hstConfiguration node or the inherited configuration
            protoTypePages = new ProtoTypePages()

            // Contribute to variables with inherited configurations if applicable
            if (parseInheritedHstConfiguration) {
                if (hstConfiguration.hasProperty("hst:inheritsfrom")) {
                    final Property inheritsFrom = hstConfiguration.getProperty("hst:inheritsfrom")
                    if (inheritsFrom) {
                        Value[] inheritedHstConfig = inheritsFrom.getValues();
                        for (Value value : inheritedHstConfig) {
                            String inheritedHst = value.getString();
                            // Clean inheritedHst to remove leading '../', './', and '/'
                            String cleanedInheritedHst = inheritedHst.replaceAll(/^(?:\.\.\/|\.\/|\/)+/, "")
                            Node inheritedNode = hstConfiguration.getParent().getNode(cleanedInheritedHst)
                            if (inheritedNode != null && inheritedNode.isNodeType("hst:configuration")) {
                                if (inheritedNode.hasNode("hst:prototypepages")) {
                                    protoTypePages.contribute(inheritedNode.getNode("hst:prototypepages"))
                                }
                            } else {
                                log.warn("Inherited HST configuration node does not exist or is not of type 'hst:configuration': " + cleanedInheritedHst)
                            }
                        }
                    }
                }
            }

            // Set variables based on the current hstConfiguration node
            if (hstConfiguration.hasNode("hst:prototypepages")) {
                protoTypePages.contribute(hstConfiguration.getNode("hst:prototypepages"))
            }
            this.hstConfigNode = hstConfiguration
            this.session = hstConfigNode.getSession()
            this.name = hstConfiguration.getName()
            this.path = hstConfiguration.getPath()
            this.hst = hstConfiguration.getParent().getParent()
            this.workspace = hstConfiguration.hasNode("hst:workspace") ? new Workspace(this, hstConfiguration.getNode("hst:workspace")) : null
            if (!hstConfiguration.hasNode("hst:sitemap")) {
                hstConfiguration.addNode("hst:sitemap", "hst:sitemap")
            }
            this.siteMap = new SiteMap(this, hstConfiguration.getNode("hst:sitemap"))
            this.xPages = getOrCreateXPages(hstConfiguration)
            this.pages = hstConfiguration.hasNode("hst:pages") ? new Pages(this, hstConfiguration.getNode("hst:pages")) : null
        }

        private XPages getOrCreateXPages(final Node hstConfiguration) throws RepositoryException {
            if (hstConfiguration.hasNode("hst:xpages")) {
                return new XPages(hstConfiguration.getNode("hst:xpages"))
            } else {
                final Node xPageContainer = hstConfiguration.addNode("hst:xpages", "hst:xpages")
                return new XPages(xPageContainer)
            }
        }

        Node getSiteBaseNode() throws RepositoryException {
            String hstSite = "hst:sites/" + name
            if (hst.hasNode(hstSite)) {
                final Node site = hst.getNode(hstSite)
                if (site.hasProperty("hst:content")) {
                    return site.getProperty("hst:content").getNode()
                }
            }
            return null
        }

        void startConversion() throws RepositoryException {
            log.info("Starting conversion of prototype page configuration to XPages for " + path)
            removeHstPreviewConfiguration()

            convertPrototypeToXPageTemplateNodes()

            createSitemapWildCards()

            createXPageContentDocuments()
            log.info("End conversion of landing pages to XPages for " + path)
        }

        void removeHstPreviewConfiguration() {
            log.info("Step 0 - Remove the preview configuration if it exists")
            final Node configurations = hstConfigNode.getParent()
            if (configurations.hasNode(hstConfigNode.getName() + "-preview")) {
                log.info("Step 0 - Removing the preview configuration, before we start the conversion")
                configurations.getNode(hstConfigNode.getName() + "-preview").remove()
            }
            log.info("Step 0 - end")
        }

        void convertPrototypeToXPageTemplateNodes() {
            log.info("Step 1 - Convert prototype pages to xpages")
            try {
                // Check if protoTypePages exists and has valid prototype pages
                if (protoTypePages != null && protoTypePages.getProtoTypePages() != null) {
                    final Map<String, XPage> xpagesFromPrototypePages = protoTypePages.getProtoTypePages().stream()
                            .filter({ protoTypePage -> protoTypePage != null }) // Filter out null prototype pages
                            .map({ protoTypePage ->
                                try {
                                    // Skip if an XPage with the same name already exists in the repository
                                    if (xPages != null && xPages.getXPages() != null && xPages.getXPages().containsKey(protoTypePage.name)) {
                                        log.info("Skipping prototype page conversion: An XPage with name '${protoTypePage.name}' already exists in the repository")
                                        return null
                                    }

                                    // Also check if the node exists directly in the XPages container node
                                    if (xPages != null && xPages.xPagesContainerNode != null &&
                                            xPages.xPagesContainerNode.hasNode(protoTypePage.name)) {
                                        log.info("Skipping prototype page conversion: An XPage node with name '${protoTypePage.name}' already exists in the repository")
                                        return null
                                    }

                                    return new XPage(protoTypePage)
                                } catch (Exception e) {
                                    log.warn("Failed to convert prototype page to XPage: ${protoTypePage?.name}", e)
                                    return null
                                }
                            })
                            .filter({ xPage -> xPage != null }) // Filter out skipped and failed conversions
                            .collect(Collectors.toMap(
                                    { e -> e.name },
                                    { e -> e },
                                    // Merge function to handle duplicate keys
                                    (existing, replacement) -> {
                                        log.warn("Duplicate XPage name found: ${existing.name}. Using the first occurrence.")
                                        return existing
                                    }
                            ))

                    log.info("Successfully mapped ${xpagesFromPrototypePages.size()} prototype pages to XPages")

                    if (xPages != null && xPages.getXPages() != null) {
                        xPages.getXPages().putAll(xpagesFromPrototypePages)
                        try {
                            xPages.createXPagesFromPrototypePages(false)
                        } catch (Exception e) {
                            log.error("Error while creating XPages from prototype pages", e)
                        }
                    } else {
                        log.error("XPages container is null or invalid, cannot add XPages")
                    }
                } else {
                    log.warn("No prototype pages found to convert")
                }
            } catch (Exception e) {
                log.error("Error in convertPrototypeToXPageTemplateNodes", e)
            }
            log.info("Step 1 - end")
        }

        void createSitemapWildCards() {
            log.info("Step 2a - Iterate through workspace to get all sitemap items")
            OptionalInt maxDepth = workspace.siteMap.getAllSiteMapItems().stream()
                    .filter({ siteMapItem -> !siteMapItem.isWildcard })
                    .mapToInt({ siteMapItem -> siteMapItem.depth })
                    .max()
            log.debug("Maximum depth of sitemap items in the workspace: " + maxDepth.orElse(-1))
            if (maxDepth.isPresent()) {
                log.info("Step 2b - based on the Maximum Depth create the _default_ sitemap items (outside of the workspace) with the xpage folder name: " + xPageFolder)
                createDevSiteMap(this, maxDepth.getAsInt())
                if (hstConfigNode.hasNode("hst:sitemap")) {
                    Node siteMapContainer = hstConfigNode.getNode("hst:sitemap")

                    // Iterate through sitemap items that are XPage sitemap items
                    for (SiteMapItem siteMapItem : siteMap.siteMapItems) {
                        if (siteMapItem.xPageSiteMapItem) {
                            try {
                                siteMapItem.toNode(siteMapContainer)
                            } catch (RepositoryException e) {
                                log.error("Error persisting the default wildcard sitemap items outside of the workspace", e)
                            }
                        }
                    }
                }
            }
            log.info("Step 2 - end")
        }

        void createXPageContentDocuments() {
            log.info("Step 3 - Create XPage content documents")


        }

        private void createDevSiteMap(final HstConfiguration hstConfiguration, final int maxDepth) {
            final List<SiteMapItem> siteMapItems = hstConfiguration.siteMap.siteMapItems
            SiteMapItem current = null
            for (int i = 0; i < maxDepth; i++) {
                if (current == null) {
                    // Create and add the root _default_ item
                    current = new SiteMapItem("_default_", xPageFolder + "/\${1}")
                    siteMapItems.add(current)
                    // Create and add the root _any_ item
                    siteMapItems.add(new SiteMapItem("_any_", xPageFolder + "/\${1}"))
                } else {
                    // Create and add the next level _default_ item and _index_ item
                    final SiteMapItem currentNextLevel = new SiteMapItem("_default_", "\${parent}/\${" + (i + 1) + "}")
                    current.siteMapItems.add(currentNextLevel)
                    current.siteMapItems.add(new SiteMapItem("_index_", "\${parent}/index"))
                    // Move to next level for next iteration
                    current = currentNextLevel
                }
            }
            log.info("populated the (default) sitemap with sitemap items")
        }
    }

    private class SiteMap {

        List<SiteMapItem> siteMapItems = new ArrayList<>()
        Workspace workspace
        HstConfiguration hstConfiguration
        String path

        SiteMap(Workspace workspace, Node node) throws RepositoryException {
            this.workspace = workspace
            init(node)
        }

        SiteMap(HstConfiguration hstConfiguration, Node node) throws RepositoryException {
            this.hstConfiguration = hstConfiguration
            init(node)
        }

        void init(Node node) throws RepositoryException {
            this.path = node.getPath()
            final NodeIterator it = node.getNodes()
            while (it.hasNext()) {
                final Node siteMapItemNode = it.nextNode()
                SiteMapItem siteMapItem = new SiteMapItem(this, siteMapItemNode)
                siteMapItems.add(siteMapItem)
            }
        }

        List<SiteMapItem> getAllSiteMapItems() {
            return siteMapItems.stream()
                    .flatMap(new Function<SiteMapItem, Stream<? extends SiteMapItem>>() {
                        @Override
                        Stream<? extends SiteMapItem> apply(final SiteMapItem siteMapItem) {
                            return siteMapItem.flattened()
                        }
                    })
                    .collect(Collectors.toList())
        }

        @Override
        String toString() {
            return "SiteMap{" +
                    "siteMapItems=" + siteMapItems +
                    ", path='" + path + '\'' +
                    '}'
        }
    }

    private class SiteMapItem implements ToNode {

        Map<String, Property> propertyMap = new HashMap<>()
        List<SiteMapItem> siteMapItems = new ArrayList<>()
        String relativeContentPath
        String name
        String pageTitle
        Page page
        SiteMap siteMap
        Node documentHandle
        String relativeSitemapPath
        String path
        int depth
        boolean isWildcard
        List<String> ignoreWithName = Arrays.asList("_default_", "_index_", "_any_")
        boolean xPageSiteMapItem

        SiteMapItem(String name, String relativeContentPath) {
            this.name = name
            this.relativeContentPath = relativeContentPath
            this.xPageSiteMapItem = true
        }

        SiteMapItem(SiteMap siteMap, Node node) throws RepositoryException {
            if (node.hasProperty("hst:relativecontentpath")) {
                log.debug("Sitemap item relative content path " + node.getProperty("hst:relativecontentpath").getString())
                node.setProperty("hst:relativecontentpath", node.getProperty("hst:relativecontentpath").getString().replaceAll("^/", ""))
            }

            this.name = node.getName()
            this.isWildcard = ignoreWithName.stream().filter({ pattern -> name.contains(pattern) }).findAny().isPresent()
            this.path = node.getPath()
            final String siteMapPath = siteMap.path
            this.relativeSitemapPath = path.replace(siteMapPath, "")
            this.depth = (int) relativeSitemapPath.chars().filter({ ch -> ch == '/' }).count()
            final PropertyIterator pit = node.getProperties("hst:*")
            while (pit.hasNext()) {
                final Property property = pit.nextProperty()
                propertyMap.put(property.getName(), property)
            }
            if (propertyMap.containsKey("hst:componentconfigurationid")) {
                final Node configurationNode = findClosestConfigurationParentNode(node)
                final Node pageNode = configurationNode.hasNode(propertyMap.get("hst:componentconfigurationid").getString()) ? configurationNode.getNode(propertyMap.get("hst:componentconfigurationid").getString()) : null
                if (pageNode != null) {
                    this.page = new Page(pageNode)
                } else {
                    log.warn("Possible invalid sitemapitem component configuration id: " + node.getPath() + " page does not exist with: " + propertyMap.get("hst:componentconfigurationid").getString() + " might be inherited!")
                }
            }
            if (propertyMap.containsKey("hst:relativecontentpath")) {
                this.relativeContentPath = propertyMap.get("hst:relativecontentpath").getString()
                final Node siteBaseNode = siteMap.workspace != null ? siteMap.workspace.hstConfiguration.getSiteBaseNode() : siteMap.hstConfiguration.getSiteBaseNode()
                if (siteBaseNode.hasNode(relativeContentPath)) {
                    documentHandle = siteBaseNode.getNode(relativeContentPath)
                }
            }
            if (propertyMap.containsKey("hst:pagetitle")) {
                this.pageTitle = propertyMap.get("hst:pagetitle").getString()
            }
            final NodeIterator it = node.getNodes()
            while (it.hasNext()) {
                final Node sitemapItemNode = it.nextNode()
                SiteMapItem siteMapItem = new SiteMapItem(siteMap, sitemapItemNode)
                siteMapItems.add(siteMapItem)
            }

        }

        Stream<SiteMapItem> flattened() {
            return Stream.concat(
                    Stream.of(this),
                    siteMapItems.stream().flatMap(new Function<SiteMapItem, Stream<? extends SiteMapItem>>() {
                        @Override
                        Stream<? extends SiteMapItem> apply(final SiteMapItem siteMapItem) {
                            return siteMapItem.flattened()
                        }
                    }))
        }

        Node findClosestConfigurationParentNode(Node node) throws RepositoryException {
            if (node.isNodeType("hst:workspace") || node.isNodeType("hst:configuration")) {
                return node
            }
            return findClosestConfigurationParentNode(node.getParent())
        }

        @Override
        String toString() {
            return "SiteMapItem{" +
                    "relativeContentPath='" + relativeContentPath + '\'' +
                    ", name='" + name + '\'' +
                    ", path='" + path + '\'' +
                    ", depth=" + depth +
                    ", isWildcard=" + isWildcard +
                    ", xPageSiteMapItem=" + xPageSiteMapItem +
                    ", pageTitle=" + pageTitle +
                    '}'
        }

        @Override
        void toNode(final Node container) throws RepositoryException {
            Node currentSiteMapItem
            if (container.hasNode(name)) {
                currentSiteMapItem = container.getNode(name)
            } else {
                currentSiteMapItem = container.addNode(name, "hst:sitemapitem")
            }
            if (relativeContentPath != null) {
                currentSiteMapItem.setProperty("hst:relativecontentpath", relativeContentPath)
            }
            siteMapItems.forEach({ siteMapItem ->
                try {
                    log.info("iterating children of sitemap: " + currentSiteMapItem.getPath())
                    siteMapItem.toNode(currentSiteMapItem)
                } catch (RepositoryException e) {
                    log.error("error...", e)
                }
            })
        }

    }

    private class Workspace {

        HstConfiguration hstConfiguration
        SiteMap siteMap
        Pages pages

        Workspace(HstConfiguration hstConfiguration, Node node) throws RepositoryException {
            this.hstConfiguration = hstConfiguration
            this.siteMap = node.hasNode("hst:sitemap") ? new SiteMap(this, node.getNode("hst:sitemap")) : null
            this.pages = node.hasNode("hst:pages") ? new Pages(this, node.getNode("hst:pages")) : null
        }
    }

    private class Page extends AbstractComponent {

        Page(final Node node) throws RepositoryException {
            super(node)
        }

        @Override
        void toNode(final Node container) throws RepositoryException {
            final Node page = container.addNode(name, "hst:page")
            addToNode(page)
        }

        List<ManageableComponent> getAllManageableComponents() {
            final List<ManageableComponent> collect = flattened()
                    .filter({ component -> component instanceof ManageableComponent })
                    .map({ component -> (ManageableComponent) component })
                    .collect(Collectors.toList())
            return collect
        }

        @Override
        String toString() {
            return "Page{" +
                    "staticComponents=" + staticComponents +
                    ", manageableComponents=" + manageableComponents +
                    '}'
        }
    }

    private class Pages {

        List<Page> pages = new ArrayList<>()

        Pages(final Workspace workspace, Node node) throws RepositoryException {
            init(node)
        }

        Pages(final HstConfiguration hstConfiguration, Node node) throws RepositoryException {
            init(node)
        }

        void init(Node node) throws RepositoryException {
            final NodeIterator it = node.getNodes()
            while (it.hasNext()) {
                final Node pageNode = it.nextNode()
                Page page = new Page(pageNode)
                pages.add(page)
            }
        }
    }

    private class ProtoTypePage extends AbstractComponent {

        final String displayName

        ProtoTypePage(Node protoTypePage) throws RepositoryException {
            super(protoTypePage)
            this.displayName = protoTypePage.isNodeType("hst:prototypemeta") && protoTypePage.hasProperty("hst:displayname") ? protoTypePage.getProperty("hst:displayname").getString() : null
        }

        @Override
        String toString() {
            return "ProtoTypePage{" +
                    "name='" + name + '\'' +
                    ", staticComponents=" + staticComponents +
                    ", manageableComponents=" + manageableComponents +
                    '}'
        }

        @Override
        void toNode(final Node Container) {
            //empty not creating any prototype pages!
        }
    }

    private class XPage extends AbstractComponent {

        String label
        String pageRef

        XPage(Node node) throws RepositoryException {
            super(node)
            this.label = node.hasProperty("hst:label") ? node.getProperty("hst:label").getString() : null
            this.pageRef = node.hasProperty("hst:pageref") ? node.getProperty("hst:pageref").getString() : null
        }

        XPage(ProtoTypePage protoTypePage) {
            super(protoTypePage)
            this.label = protoTypePage.displayName
            propertyMap.remove("hst:displayname")
            propertyMap.remove("hst:primarycontainer")
            log.info("conversion of prototype page: " + protoTypePage.name + " to xpage: " + name)
        }

        void toNode(Node container) throws RepositoryException {
            if (container.hasNode(name)) {
                container.getNode(name).remove()
            }
            final Node xPageNode = container.addNode(name, "hst:xpage")
            addToNode(xPageNode)
            log.debug("creating node structure of xpage: " + name)
        }

        @Override
        Node addToNode(final Node node) throws RepositoryException {
            Node added = super.addToNode(node)
            if (label != null) {
                added.setProperty("hst:label", label)
            }
            if (pageRef != null) {
                added.setProperty("hst:pageref", pageRef)
            }
            if (!manageableComponents.isEmpty()) {
                manageableComponents.forEach({ manageableComponent ->
                    try {
                        manageableComponent.toNode(added)
                    } catch (RepositoryException e) {
                        log.error("error...", e)
                    }
                })
            }
            return added
        }

        @Override
        String toString() {
            return "Page{" +
                    "staticComponents=" + staticComponents +
                    ", manageableComponents=" + manageableComponents +
                    '}'
        }
    }

    private abstract class AbstractComponent implements ToNode {

        String name
        String path
        List<StaticComponent> staticComponents = new ArrayList<>()
        List<ManageableComponent> manageableComponents = new ArrayList<>()
        Map<String, Property> propertyMap = new HashMap<>()
        List<String> ignoreWithName = Arrays.asList("hst:lastmodified")

        AbstractComponent(AbstractComponent component) {
            name = component.name
            staticComponents = component.staticComponents
            manageableComponents = component.manageableComponents
            propertyMap = component.propertyMap
        }

        AbstractComponent(Node node) throws RepositoryException {
            final PropertyIterator pit = node.getProperties("hst:*")
            while (pit.hasNext()) {
                final Property property = pit.nextProperty()
                propertyMap.put(property.getName(), property)
            }
            this.name = node.getName()
            this.path = node.getPath()
            final NodeIterator it = node.getNodes()
            while (it.hasNext()) {
                final Node component = it.nextNode()
                if (component.isNodeType("hst:component")) {
                    StaticComponent staticComponent = new StaticComponent(component)
                    staticComponents.add(staticComponent)
                } else if (component.isNodeType("hst:containercomponent")) {
                    ManageableComponent manageableComponent = new ManageableComponent(component)
                    manageableComponents.add(manageableComponent)
                }
            }
        }

        Stream<AbstractComponent> flattened() {
            return Stream.concat(
                    Stream.concat(Stream.of(this),
                            manageableComponents.stream().flatMap({ manageableComponent -> manageableComponent.flattened() })),
                    Stream.concat(Stream.of(this),
                            staticComponents.stream().flatMap({ staticComponent -> staticComponent.flattened() })))
        }

        void setName(final String name) {
            this.name = name
        }

        Node addToNode(Node node) throws RepositoryException {
            propertyMap.forEach({ propertyName, property ->
                try {
                    if (property.isMultiple()) {
                        node.setProperty(propertyName, property.getValues())
                    } else {
                        node.setProperty(propertyName, property.getValue())
                    }
                } catch (RepositoryException e) {
                    log.error("error while adding hst properties to the node", e)
                }
            })
            if (!staticComponents.isEmpty()) {
                staticComponents.forEach({ staticComponent ->
                    try {
                        staticComponent.toNode(node)
                    } catch (RepositoryException e) {
                        log.error(e.getLocalizedMessage(), e)
                    }
                })
            }
            if (!manageableComponents.isEmpty()) {
                manageableComponents.forEach({ manageableComponent ->
                    try {
                        manageableComponent.toNode(node)
                    } catch (RepositoryException e) {
                        log.error(e.getLocalizedMessage(), e)
                    }
                })
            }
            return node
        }

        String propertyMapToString() {
            return propertyMap.entrySet().stream()
                    .filter({ propertyEntry -> !ignoreWithName.contains(propertyEntry.getKey()) })
                    .map({ stringPropertyEntry ->
                        final Property property = stringPropertyEntry.getValue()
                        String stringValue
                        if (property.isMultiple()) {
                            stringValue = Stream.of(property.getValues()).map({ value -> value.getString() })
                                    .collect(Collectors.joining(", ", "{", "}"))
                        } else {
                            stringValue = property.getValue().getString()
                        }
                        return stringPropertyEntry.getKey() + "=" + stringValue
                    })
                    .collect(Collectors.joining(", ", "{", "}"))
        }

        @Override
        String toString() {
            return "AbstractComponent{" +
                    "name='" + name + '\'' +
                    "properties='" + propertyMapToString() + '\'' +
                    ", staticComponents=" + staticComponents +
                    ", manageableComponents=" + manageableComponents +
                    '}'
        }
    }

    private class XPages {

        final Map<String, XPage> xPages = new HashMap<>()
        final Node xPagesContainerNode

        XPages(Node container) throws RepositoryException {
            this.xPagesContainerNode = container
            final NodeIterator it = container.getNodes()
            while (it.hasNext()) {
                final Node xPage = it.nextNode()
                if (xPage.isNodeType("hst:xpage")) {
                    XPage page = new XPage(xPage)
                    xPages.put(xPage.getName(), page)
                }
            }
        }

        Map<String, XPage> getXPages() {
            return xPages
        }

        /**
         * Creates XPages in the repository from prototype pages.
         *
         * @param overwriteExisting if false, existing XPage nodes will be skipped and not overwritten
         */
        void createXPagesFromPrototypePages(boolean overwriteExisting = true) {
            getXPages().values().stream().forEach(xPage -> {
                try {
                    // Check if node already exists in the repository
                    if (!overwriteExisting && xPagesContainerNode.hasNode(xPage.name)) {
                        log.debug("Skipping persisting xpage " + xPage.name + " as it already exists in JCR repository");
                    } else {
                        xPage.toNode(xPagesContainerNode);
                        log.debug("Persisting xpage " + xPage.name + " to JCR repository");
                    }
                } catch (RepositoryException e) {
                    log.warn("Error persisting xpage to JCR repository", e);
                }
            });
            log.info("Done persisting all xpages to the JCR repository");
        }
    }

    private class ProtoTypePages {
        final List<ProtoTypePage> protoTypePages = new ArrayList<>()

        ProtoTypePages(Node... containers) throws RepositoryException {
            contribute(containers)
        }

        void contribute(Node... containers) throws RepositoryException {
            for (Node container : containers) {
                final NodeIterator it = container.getNodes()
                while (it.hasNext()) {
                    final Node protoTypePage = it.nextNode()
                    if (protoTypePage.isNodeType("hst:component")) {
                        ProtoTypePage page = new ProtoTypePage(protoTypePage)
                        protoTypePages.add(page)
                    }
                }
            }
        }

        List<ProtoTypePage> getProtoTypePages() {
            return protoTypePages
        }
    }

    private class StaticComponent extends AbstractComponent implements ToNode {

        StaticComponent(Node node) throws RepositoryException {
            super(node)
        }

        @Override
        String toString() {
            return "StaticComponent{" +
                    "name='" + name + '\'' +
                    ", properties='" + propertyMapToString() + '\'' +
                    ", staticComponents=" + staticComponents +
                    ", manageableComponents=" + manageableComponents +
                    '}'
        }

        @Override
        void toNode(final Node container) throws RepositoryException {
            if (container.hasNode(name)) {
                container.getNode(name).remove()
            }
            final Node node = container.addNode(name, "hst:component")
            addToNode(node)
        }
    }

    private class DynamicComponent extends AbstractComponent {

        DynamicComponent(final Node node) throws RepositoryException {
            super(node)
        }

        @Override
        void toNode(final Node container) throws RepositoryException {
            if (container.hasNode(name)) {
                container.getNode(name).remove()
            }
            final Node node = container.addNode(name, "hst:containeritemcomponent")
            addToNode(node)
        }
    }

    private class ManageableComponent extends AbstractComponent {

        List<DynamicComponent> dynamicComponents = new ArrayList<>()

        ManageableComponent(Node node) throws RepositoryException {
            super(node)
            final NodeIterator it = node.getNodes()
            while (it.hasNext()) {
                final Node dynamicComponentNode = it.nextNode()
                if (dynamicComponentNode.isNodeType("hst:containeritemcomponent")) {
                    DynamicComponent dynamicComponent = new DynamicComponent(dynamicComponentNode)
                    dynamicComponents.add(dynamicComponent)
                }
            }
        }

        @Override
        String toString() {
            return "ManageableComponent{" +
                    "name='" + name + '\'' +
                    ", properties='" + propertyMapToString() + '\'' +
                    '}'
        }

        @Override
        void toNode(final Node container) throws RepositoryException {
            if (container.hasNode(name)) {
                container.getNode(name).remove()
            }
            final Node node = container.addNode(name, "hst:containercomponent")
            addToNode(node)
            if (!dynamicComponents.isEmpty()) {
                dynamicComponents.forEach({ dynamicComponent ->
                    try {
                        dynamicComponent.toNode(node)
                    } catch (RepositoryException e) {
                        log.error("error..", e)
                    }
                })
            }
        }
    }
}
