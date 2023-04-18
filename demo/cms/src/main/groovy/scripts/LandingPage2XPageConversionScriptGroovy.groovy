package org.bloomreach.cms.scripts

import org.apache.commons.lang3.RandomStringUtils
import org.apache.commons.lang3.StringUtils
import org.hippoecm.hst.configuration.model.HstNode
import org.hippoecm.hst.platform.configuration.cache.HstNodeImpl
import org.hippoecm.hst.platform.configuration.components.JcrTemplateNodeConverter
import org.hippoecm.hst.util.PathUtils
import org.hippoecm.repository.api.HippoWorkspace
import org.hippoecm.repository.api.WorkflowException
import org.hippoecm.repository.standardworkflow.FolderWorkflow
import org.hippoecm.repository.standardworkflow.JcrTemplateNode
import org.onehippo.repository.documentworkflow.DocumentHandle
import org.onehippo.repository.documentworkflow.DocumentVariant
import org.onehippo.repository.documentworkflow.DocumentWorkflow
import org.onehippo.repository.update.BaseNodeUpdateVisitor

import javax.jcr.*
import java.rmi.RemoteException
import java.util.function.Function
import java.util.stream.Collectors
import java.util.stream.Stream

class LandingPage2XPageConversionScriptGroovy extends BaseNodeUpdateVisitor {

    String hstConfiguration
    String xPageFolder
    String defaultXpageDocType

    void initialize(Session session) {
        hstConfiguration = (String) parametersMap.get("hstConfiguration")
        xPageFolder = (String) parametersMap.getOrDefault("xPageFolder", "xpages")
        defaultXpageDocType = (String) parametersMap.get("defaultXpageDocType")
        if (StringUtils.isEmpty(hstConfiguration)) {
            log.error("hstConfiguration property is empty")
            throw new RuntimeException("hstConfiguration property is empty")
        }
        if (StringUtils.isEmpty(defaultXpageDocType)) {
            log.error("defaultXpageDocType property is empty")
            throw new RuntimeException("defaultXpageDocType property is empty")
        }
        log.info("starting the landing 2 xpage conversion script with configuration" +
                " hstConfiguration=" + hstConfiguration +
                ", xPageFolder=" + xPageFolder +
                ", defaultXpageDocType=" + defaultXpageDocType)
    }

    @Override
    boolean doUpdate(final Node node) throws RepositoryException {
        if (node.isNodeType("hst:configurations")) {
            if (node.hasNode(hstConfiguration)) {
                final NodeIterator it = node.getNodes(hstConfiguration)
                while (it.hasNext()) {
                    final Node configuration = it.nextNode()
                    if (configuration.isNodeType("hst:configuration")) {
                        log.info("Initializing the in-memory channel configuration model")
                        HstConfiguration hstConfig = new HstConfiguration(configuration)
                        hstConfig.startConversion()
                    }
                }
            }
        }
        return true
    }

    @Override
    boolean undoUpdate(final Node node) throws RepositoryException, UnsupportedOperationException {
        return false
    }


    interface ToNode {
        void toNode(Node container) throws RepositoryException;
    }

    private class HstConfiguration {

        final ProtoTypePages protoTypePages
        final Workspace workspace
        final SiteMap siteMap
        final XPages xPages
        final String name
        final String path
        final Node hst
        final Node hstConfigNode
        final Pages pages
        final Session session;

        HstConfiguration(Node hstConfiguration) throws RepositoryException {
            this.hstConfigNode = hstConfiguration
            this.session = hstConfigNode.getSession();
            this.name = hstConfiguration.getName()
            this.path = hstConfiguration.getPath()
            this.hst = hstConfiguration.getParent().getParent()
            this.protoTypePages = hstConfiguration.hasNode("hst:prototypepages") ? new ProtoTypePages(hstConfiguration.getNode("hst:prototypepages")) : null
            this.workspace = hstConfiguration.hasNode("hst:workspace") ? new Workspace(this, hstConfiguration.getNode("hst:workspace")) : null
            this.siteMap = hstConfiguration.hasNode("hst:sitemap") ? new SiteMap(this, hstConfiguration.getNode("hst:sitemap")) : null
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
            log.info("Starting conversion of landing pages to XPages for " + path)
            executeStep0();
            session.save();
            executeStep1();
            session.save();
            executeStep2();
            session.save();
            executeStep3();
            session.save();
            executeStep4();
            log.info("End conversion of landing pages to XPages for " + path)
            //step5b cleanup all pages which have no reference from the sitemapitems
        }

        Comparator<SiteMapItem> getReverseDepthComparator() {
            return new Comparator<SiteMapItem>() {
                @Override
                int compare(final SiteMapItem o1, final SiteMapItem o2) {
                    return o2.depth <=> o1.depth;
                }
            }
        }


        DocumentWorkflow getDocumentWorflow(Node documentHandle) {
            return (DocumentWorkflow) ((HippoWorkspace) session.getWorkspace()).getWorkflowManager().getWorkflow("default", documentHandle);
        }

        FolderWorkflow getFolderWorkflow(Node folder) {
            return (FolderWorkflow) ((HippoWorkspace) session.getWorkspace()).getWorkflowManager().getWorkflow("threepane", folder)
        }

        private void applyXpageToDocument(Node handleNode, documentXpageManageableComponents, xPageByPage) {
            DocumentHandle handle = new DocumentHandle(handleNode)
            handle.initialize()
            final Map<String, DocumentVariant> documents = handle.getDocuments()

            documents.entrySet()
                    .stream()
                    .filter({ documentVariant -> ("draft" != documentVariant.getKey()) })
                    .forEach({ documentVariant ->
                        final Node documentNode = documentVariant.getValue().getNode()
                        try {
                            Node xpage
                            if (!documentNode.isNodeType("hst:xpagemixin") && documentNode.canAddMixin("hst:xpagemixin")) {
                                log.info("adding pagemixin to document: " + documentNode.getPath())
                                documentNode.addMixin("hst:xpagemixin")
                            }
                            if (!documentNode.hasNode("hst:xpage")) {
                                log.info("adding hst:xpage to document: " + documentNode.getPath())
                                xpage = documentNode.addNode("hst:xpage", "hst:xpage")
                            } else {
                                log.info("getting hst:xpage from document: " + documentNode.getPath())
                                xpage = documentNode.getNode("hst:xpage")
                                log.info("removing all underlying xpage configuration " + documentNode.getPath())
                                final NodeIterator it = xpage.getNodes()
                                while (it.hasNext()) {
                                    it.nextNode().remove()
                                }
                            }

                            final List<ManageableComponent> componentList = documentXpageManageableComponents.stream()
                                    .map({ manageableComponent ->
                                        final ManageableComponent copy = new ManageableComponent(manageableComponent)
                                        copy.setName(manageableComponent.identifier)
                                        copy.identifier = UUID.randomUUID().toString()
                                        return copy
                                    }).collect(Collectors.toList())
                            XPage documentXpage = new XPage(xpage)
                            documentXpage.manageableComponents = componentList
                            documentXpage.pageRef = xPageByPage.name
                            documentXpage.addToNode(xpage)
                        } catch (RepositoryException e) {
                            log.error("error while trying to convert a landing page to xpage", e)
                        }
                    })

        }

        Node getOrCreateDeepestXpageFolder(Node siteBaseNode, String path) throws RepositoryException {
            boolean isTranslated = siteBaseNode.isNodeType("hippotranslation:translated");
            log.info("base folder is translated: " + isTranslated);
            String locale = isTranslated ? siteBaseNode.getProperty("hippotranslation:locale").getString() : null;
            Node xPageFolderNode;
            String[] folderType = ['new-document']
            if (siteBaseNode.hasNode(xPageFolder)) {
                log.info("root xpage folder does exits")
                xPageFolderNode = siteBaseNode.getNode(xPageFolder);
                if (!xPageFolderNode.isNodeType("hippostd:folder")) {
                    log.info("removing root xpage folder")
                    xPageFolderNode.remove();
                    return getOrCreateDeepestXpageFolder(siteBaseNode, path);
                }
            } else {
                log.info("root xpage folder does not exits, creating now..")
                xPageFolderNode = siteBaseNode.addNode(xPageFolder, "hippostd:folder");
                xPageFolderNode.setProperty("hippostd:foldertype", folderType);
            }

            translate(isTranslated, locale, xPageFolderNode);

            if (!xPageFolderNode.isNodeType("hippostd:xpagefolder")) {
                xPageFolderNode.addMixin("hippostd:xpagefolder");
                xPageFolderNode.setProperty("hippostd:channelid", "myproject");
            }

            if (StringUtils.isNotEmpty(path) && path.contains("/")) {
                final List<String> paths = new ArrayList<>(Arrays.asList(path.split("/")));
                paths.remove(paths.size() - 1);
                for (String segment : paths) {
                    if (xPageFolderNode.hasNode(segment)) {
                        Node temp = xPageFolderNode.getNode(segment);
                        if (temp.isNodeType("hippostd:folder") && temp.isNodeType("hippostd:xpagefolder")) {
                            xPageFolderNode = temp;
                        } else if (temp.isNodeType("hippo:handle")) {
                            String id = RandomStringUtils.random(5, true, false);
                            String originalName = temp.getName();
                            String tempName = originalName + "-" + id;
                            String originalTempPath = temp.getPath();
                            String newTempPath = temp.getParent().getPath() + tempName;
                            session.move(originalTempPath, newTempPath);
                            xPageFolderNode = xPageFolderNode.addNode(segment, "hippostd:folder");
                            xPageFolderNode.setProperty("hippostd:foldertype", folderType);
                            translate(isTranslated, locale, xPageFolderNode);
                            if (!xPageFolderNode.isNodeType("hippostd:xpagefolder")) {
                                xPageFolderNode.addMixin("hippostd:xpagefolder");
                                xPageFolderNode.setProperty("hippostd:channelid", "myproject");
                            }
                            session.move(newTempPath, xPageFolderNode.getPath() + "/index");
                            final Node index = xPageFolderNode.getNode("index");
                            final NodeIterator it = index.getNodes();
                            while (it.hasNext()) {
                                final Node doc = it.nextNode();
                                if (doc.isNodeType("hippo:document")) {
                                    rename(doc, "index");
                                }
                            }
                        }
                    } else {
                        xPageFolderNode = xPageFolderNode.addNode(segment, "hippostd:folder");
                        if (!xPageFolderNode.isNodeType("hippostd:xpagefolder")) {
                            xPageFolderNode.addMixin("hippostd:xpagefolder");
                            xPageFolderNode.setProperty("hippostd:channelid", "myproject");
                        }
                        translate(isTranslated, locale, xPageFolderNode);
                    }
                }
            }
            return xPageFolderNode;
        }

        void translate(boolean isTranslated, String locale, Node xPageFolderNode){
            if(isTranslated){
                xPageFolderNode.addMixin("hippotranslation:translated")
                xPageFolderNode.setProperty("hippotranslation:locale", locale);
                xPageFolderNode.setProperty("hippotranslation:id", UUID.randomUUID().toString());
            }
        }

        void rename(Node node, String newName) throws RepositoryException {
            session.move(node.getPath(), node.getParent().getPath() + "/" + newName);
        }

        private void createDevSiteMap(final HstConfiguration hstConfiguration, final int maxDepth) {
            final List<SiteMapItem> siteMapItems = hstConfiguration.siteMap.siteMapItems
            SiteMapItem current = null
            for (int i = 0; i < maxDepth; i++) {
                if (current == null) {
                    current = new SiteMapItem("_default_", xPageFolder + "/\${" + (i + 1) + "}")
                    siteMapItems.add(current)
                } else {
                    final SiteMapItem currentNexLevel = new SiteMapItem("_default_", "\${parent}/\${" + (i + 1) + "}")
                    new SiteMapItem("_default_", "\${parent}/\${" + (i + 1) + "}")
                    current.siteMapItems.add(currentNexLevel)
                    current.siteMapItems.add(new SiteMapItem("_index_", "\${parent}/index"))
                    current = currentNexLevel
                }
            }
            log.info("populated the (default) sitemap with sitemap items")
        }

        void resetDocumentThroughWorkflow(DocumentWorkflow documentWorkflow) {
            documentWorkflow.obtainEditableInstance()
            documentWorkflow.commitEditableInstance()
        }

        void executeStep3() {
            log.info("Step 3a - iterate through workspace to get all sitemap items")
            OptionalInt maxDepth = workspace.siteMap.getAllSiteMapItems().stream()
                    .filter({ siteMapItem -> !siteMapItem.isWildcard })
//                    .filter({ siteMapItem -> siteMapItem.relativeContentPath != null })
                    .mapToInt({ siteMapItem -> siteMapItem.depth })
                    .max();
            if (maxDepth.isPresent()) {
                log.info("Maximum sitemap items depth in the workspace = " + maxDepth.getAsInt())
                log.info("Step 3b - based on the Maximum Depth create the _default_ sitemap items (outside of the workspace) with the xpage folder name: " + xPageFolder)
                createDevSiteMap(this, maxDepth.getAsInt())
                if (hstConfigNode.hasNode("hst:sitemap")) {
                    Node siteMapContainer = hstConfigNode.getNode("hst:sitemap")
                    siteMap.siteMapItems.stream().filter({ siteMapItem -> siteMapItem.xPageSiteMapItem }).forEach({ siteMapItem ->
                        try {
                            siteMapItem.toNode(siteMapContainer)
                        } catch (RepositoryException e) {
                            log.error("Error persisting the _default_ wildcard sitemap items outside of the workspace", e)
                        }
                    })
                }
            }
            log.info("Step 3 - end")
        }

        void executeStep4() {
            log.info("Step 4 - Creating XPages from landingpages without a relative content path, this will create xpage documents in the XPage folder: " + xPageFolder)
            workspace.siteMap.getAllSiteMapItems().stream()
                    .filter({ siteMapItem -> siteMapItem.relativeContentPath == null && siteMapItem.page != null })
                    .sorted(getReverseDepthComparator())
                    .forEach({ siteMapItem ->
                        try {
                            log.info("sitemapitem without relative content path " + siteMapItem.path)
                            final Node siteBaseNode = getSiteBaseNode()

                            Node xPageFolder = getOrCreateDeepestXpageFolder(siteBaseNode, PathUtils.normalizePath(siteMapItem.relativeSitemapPath));

                            session.save();
                            final Page currentLandingPage = siteMapItem.page
                            final Optional<XPage> xPageTemplate = xPages.findXPageTemplateByPage(currentLandingPage);

                            if (xPageFolder != null && xPageTemplate.isPresent()) {
                                final List<ManageableComponent> currentLandingPageManageableComponents = currentLandingPage.getAllManageableComponents()
                                Node handleNode

                                def xPageTemplateItem = xPageTemplate.get();

                                if (!xPageFolder.hasNode(siteMapItem.name)) {
                                    log.info("trying to add document..");
                                    log.info(xPageFolder.getPath());
                                    FolderWorkflow folderWorkflow = getFolderWorkflow(xPageFolder);
                                    log.info("folderworkflow is available");
                                    final Node xPageTemplateNode = hstConfigNode.getNode("hst:xpages").getNode(xPageTemplateItem.name)
                                    HstNode xpageNode = new HstNodeImpl(xPageTemplateNode, null)
                                    final JcrTemplateNode xPageLaoutAsJcrTemplate = JcrTemplateNodeConverter.getXPageLaoutAsJcrTemplate(xpageNode)
                                    log.info("preparing the xpage template");
                                    final String newDocument = folderWorkflow.add("new-document", defaultXpageDocType, siteMapItem.name, xPageLaoutAsJcrTemplate)
                                    log.info("added document" + newDocument);
                                    handleNode = session.getNode(newDocument).getParent()
                                    DocumentWorkflow documentWorkflow = getDocumentWorflow(handleNode);
                                    resetDocumentThroughWorkflow(documentWorkflow);
                                } else if (xPageFolder.getNode(siteMapItem.name).isNodeType("hippostd:folder")) {
                                    log.info("folder exists! returning existing document")
                                    xPageFolder = xPageFolder.getNode(siteMapItem.name);
                                    FolderWorkflow folderWorkflow = getFolderWorkflow(xPageFolder)
                                    log.info("folderworkflow is available");
                                    final Node xPageTemplateNode = hstConfigNode.getNode("hst:xpages").getNode(xPageTemplateItem.name)
                                    HstNode xpageNode = new HstNodeImpl(xPageTemplateNode, null)
                                    final JcrTemplateNode xPageLaoutAsJcrTemplate = JcrTemplateNodeConverter.getXPageLaoutAsJcrTemplate(xpageNode)
                                    log.info("preparing the xpage template");
                                    final String newDocument = folderWorkflow.add("new-document", defaultXpageDocType, "index", xPageLaoutAsJcrTemplate)
                                    log.info("added document" + newDocument);
                                    handleNode = session.getNode(newDocument).getParent()
                                    DocumentWorkflow documentWorkflow = getDocumentWorflow(handleNode)
                                    resetDocumentThroughWorkflow(documentWorkflow);
                                } else if (xPageFolder.getNode(siteMapItem.name).isNodeType("hippo:handle")) {
                                    handleNode = xPageFolder.getNode(siteMapItem.name)
                                    DocumentWorkflow documentWorkflow = getDocumentWorflow(handleNode)
                                    resetDocumentThroughWorkflow(documentWorkflow);
                                }

                                applyXpageToDocument(handleNode, currentLandingPageManageableComponents, xPageTemplateItem)

                                //cleanup sitemap item reference to component configuration id
                                if (session.nodeExists(siteMapItem.path)) {
                                    log.info("cleaning up sitemap item")
                                    Node siteMapItemNode = session.getNode(siteMapItem.path);
                                    if (!siteMapItemNode.getNodes().hasNext()) {
                                        log.info("node has sub nodes so it will be removed")
                                        siteMapItemNode.remove();
                                    } else {
                                        log.info("cannot remove node because it has subnodes, will need to a custom relative content path")
                                        if (siteMapItemNode.hasProperty("hst:componentconfigurationid")) {
                                            siteMapItemNode.getProperty("hst:componentconfigurationid").remove()
                                        }
                                        siteMapItemNode.setProperty("hst:relativecontentpath", PathUtils.normalizePath(handleNode.getPath().replace(siteBaseNode.getPath(), "")));
                                    }
                                }

                                if (session.nodeExists(currentLandingPage.path)) {
                                    log.info("cleaning up workspace page: " + currentLandingPage.path)
                                    session.getNode(currentLandingPage.path).remove();
                                }
                                log.info("updated:" + siteMapItem.path)
                            }
                        } catch (RepositoryException | RemoteException | WorkflowException e) {
                            log.error("error in step 4, it could be that the workflow did not have access to the intermediate folders, please run script again", e)
                        }
                    })
            log.info("Step 4 - end")
        }

        void executeStep2() {
            log.info("Step 2 - Convert all sitemap items which have a primary (a.k.a relative content path) document configured on the sitemap items in the workspace")
            workspace.siteMap.getAllSiteMapItems().stream()
                    .filter({ siteMapItem -> siteMapItem.relativeContentPath != null && siteMapItem.page != null })
                    .forEach({ siteMapItem ->
                        try {
                            final String relativeContentPath = siteMapItem.relativeContentPath
                            log.info("starting with :" + siteMapItem.path)
                            final Node siteBaseNode = getSiteBaseNode();
                            final Page currentLandingPage = siteMapItem.page
                            final Optional<XPage> xPageTemplate = xPages.findXPageTemplateByPage(currentLandingPage)
                            if (siteBaseNode.hasNode(relativeContentPath) && xPageTemplate.isPresent()) {
                                final List<ManageableComponent> currentLandingPageManageableComponents = currentLandingPage.getAllManageableComponents()

                                final Node handleNode = siteBaseNode.getNode(relativeContentPath)

                                applyXpageToDocument(handleNode, currentLandingPageManageableComponents, xPageTemplate.get());

                                final Node sitemapitemNode = session.getNode(siteMapItem.path)
                                log.info("cleaning up sitemap item")
                                sitemapitemNode.getProperty("hst:componentconfigurationid").remove()

                                log.info("cleaning up workspace page: " + currentLandingPage.path)
                                final Node pageNode = session.getNode(currentLandingPage.path)
                                pageNode.remove()
                                log.info("updated:" + siteMapItem.path)
                            } else {
                                log.warn("non existing relative content path, please fix: " + siteBaseNode.getPath() + "/" + relativeContentPath)
                            }
                        } catch (RepositoryException | WorkflowException e) {
                            log.error("error", e)
                        }
                    })
            log.info("Step 2 - end")

        }

        void executeStep1() {
            log.info("Step 1 - convert prototype pages to xpages")
            final Map<String, XPage> xpagesFromPrototypePages = protoTypePages.getProtoTypePages().stream()
                    .map({ protoTypePage -> new XPage(protoTypePage) }).collect(Collectors.toMap(
                    { e -> e.name },
                    { e -> e }
            ))
            xPages.getXPages().putAll(xpagesFromPrototypePages)
            xPages.createXPagesFromPrototypePages()
            log.info("Step 1 - end")
        }

        void executeStep0() {
            final Node configurations = hstConfigNode.getParent()
            if (configurations.hasNode(hstConfiguration + "-preview")) {
                //todo step 0, preview configuration REMOVE by force
                log.info("Step 0 - removing the preview configuration, before we start the conversion")
                configurations.getNode(hstConfiguration + "-preview").remove()
            }
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
                            return siteMapItem.flattened();
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
                            return siteMapItem.flattened();
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
            log.info("creating node structure of xpage: " + name)
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
                        log.error("error...", e);
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
                    log.error("error while adding hst properties to the node", e);
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

        Optional<XPage> findXPageTemplateByPage(Page page) {
            final String pageIdentifier = page.toString()
            final Optional<XPage> first = xPages.values().stream()
                    .filter({ xPage ->
                        (pageIdentifier == xPage.toString())
                    })
                    .findFirst()
            return first;
        }

        Map<String, XPage> getXPages() {
            return xPages
        }

        void createXPagesFromPrototypePages() {
            getXPages().values().stream().forEach({ xPage ->
                try {
                    xPage.toNode(xPagesContainerNode)
                    log.info("persisting xpage " + xPage.name + " to JCR repository")
                } catch (RepositoryException e) {
                    log.error("Error persisting xpathe to JCR repository", e)
                }
            })
            log.info("Done persisting all xpages to the JCR repository")
        }
    }

    private class ProtoTypePages {

        final List<ProtoTypePage> protoTypePages = new ArrayList<>()

        ProtoTypePages(Node container) throws RepositoryException {
            final NodeIterator it = container.getNodes()
            while (it.hasNext()) {
                final Node protoTypePage = it.nextNode()
                if (protoTypePage.isNodeType("hst:component")) {
                    ProtoTypePage page = new ProtoTypePage(protoTypePage)
                    protoTypePages.add(page)
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

        String identifier
        List<DynamicComponent> dynamicComponents = new ArrayList<>()

        ManageableComponent(Node node) throws RepositoryException {
            super(node)
            this.identifier = node.hasProperty("hippo:identifier") ? node.getProperty("hippo:identifier").getString() : null
            final NodeIterator it = node.getNodes()
            while (it.hasNext()) {
                final Node dynamicComponentNode = it.nextNode()
                if (dynamicComponentNode.isNodeType("hst:containeritemcomponent")) {
                    DynamicComponent dynamicComponent = new DynamicComponent(dynamicComponentNode)
                    dynamicComponents.add(dynamicComponent)
                }
            }
        }

        ManageableComponent(ManageableComponent manageableComponent) {
            super(manageableComponent)
            this.dynamicComponents = manageableComponent.dynamicComponents
        }

        @Override
        String toString() {
            return "ManageableComponent{" +
                    "name='" + name + '\'' +
                    ", properties='" + propertyMapToString() + '\'' +
                    ", identifier='" + identifier + '\'' +
                    '}'
        }

        @Override
        void toNode(final Node container) throws RepositoryException {
            if (container.hasNode(name)) {
                container.getNode(name).remove()
            }
            final Node node = container.addNode(name, "hst:containercomponent")
            addToNode(node)
            if (identifier != null) {
                node.setProperty("hippo:identifier", identifier)
            }
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
