/*
 * Landing page -> Experience Page (XPage) conversion for brXM 17.x
 *
 * Runs as a repository updater (Setup > System > Updater editor). Select ONLY the hst:configurations node:
 *   XPath query: /jcr:root/<hstRoot>/hst:configurations     e.g. /jcr:root/hst:myproject/hst:configurations
 *   (do not use "Repository path": that visits every HST node below it and is slowed down by the throttle)
 *   The runtime HST root is the site's "hstRoot" (hcm-site.yaml / hst.configuration.rootPath), NOT the /hst:hst
 *   prefix used inside HCM YAML files.
 *
 * The conversion is split into steps that are run one (or a few) at a time with the "steps" parameter:
 *
 *   analyze    (default, read-only)  report what would happen for every workspace sitemap item
 *   templates  (writes HST config)   convert hst:prototypepages into hst:xpages layouts (+ optional generated layouts)
 *   sitemap    (writes HST config)   add a hidden "<xPageFolder>/**" sitemap subtree so NEW editor-created XPages resolve
 *   convert    (writes content)      create an XPage document per landing page and copy the container items into it
 *   cleanup    (writes HST config)   repoint converted sitemap items to their XPage document and remove orphaned pages
 *
 * Recommended order: analyze -> templates -> analyze -> convert -> verify in Experience manager -> cleanup.
 * Nothing is deleted before "cleanup", and every step can be re-run safely.
 *
 * The updater "dry run" checkbox is honored: a dry run only ever executes "analyze".
 *
 * See README.md next to this script for the full parameter reference.
 */

import org.apache.commons.lang3.StringUtils
import org.hippoecm.hst.configuration.model.HstNode
import org.hippoecm.hst.platform.configuration.cache.HstNodeImpl
import org.hippoecm.hst.platform.configuration.components.JcrTemplateNodeConverter
import org.hippoecm.repository.api.HippoWorkspace
import org.hippoecm.repository.api.NodeNameCodec
import org.hippoecm.repository.api.Workflow
import org.hippoecm.repository.standardworkflow.FolderWorkflow
import org.hippoecm.repository.standardworkflow.JcrTemplateNode
import org.onehippo.repository.documentworkflow.DocumentWorkflow
import org.onehippo.repository.update.BaseNodeUpdateVisitor

import javax.jcr.Node
import javax.jcr.NodeIterator
import javax.jcr.Property
import javax.jcr.PropertyIterator
import javax.jcr.PropertyType
import javax.jcr.RepositoryException
import javax.jcr.Session
import javax.jcr.Value

class LandingPageToXPageConverter extends BaseNodeUpdateVisitor {

    // ------------------------------------------------------------------------------------------------------------
    // constants
    // ------------------------------------------------------------------------------------------------------------

    static final String VERSION = "2.0.0"
    static final List<String> STEP_ORDER = ["analyze", "templates", "sitemap", "convert", "cleanup"]
    static final List<String> MUTATING_STEPS = ["templates", "sitemap", "convert", "cleanup"]
    static final List<String> WILDCARD_MARKERS = ["_default_", "_any_", "_index_", '${']
    /** properties never copied from landing pages / prototypes into layouts or documents */
    static final List<String> NON_COPY_PROPERTIES = ["hst:lockedby", "hst:lockedon", "hst:lastmodified", "hst:lastmodifiedby"]
    /** properties of a prototype page that do not belong on an hst:xpage layout */
    static final List<String> PROTOTYPE_ONLY_PROPERTIES = ["hst:primarycontainer", "hst:displayname"]
    static final int LOG_SUMMARY = 0, LOG_ITEM = 1, LOG_TRACE = 2

    // ------------------------------------------------------------------------------------------------------------
    // parameters (see README.md)
    // ------------------------------------------------------------------------------------------------------------

    List<String> hstConfigurations
    List<String> steps
    String docType
    String primaryDocumentMode
    String xPageFolder
    String documentTemplateCategory
    List<String> folderTypes
    String contentRootPath
    Map<String, String> templateMapping
    String fallbackXPageTemplate
    String unmatchedContainers
    String existingDocument
    boolean publish
    boolean publishPendingChanges
    String titleProperty
    boolean setDisplayName
    boolean parseInheritedHstConfiguration
    boolean overwriteXPageTemplates
    boolean generateMissingTemplates
    boolean discardPreviewConfiguration
    boolean cleanupUnpublished
    List<String> includePaths
    int limit
    int logLevel
    int progressEvery
    boolean reportToRepository

    boolean dryRun
    Session session
    final Set<String> visitedConfigurations = new HashSet<>()

    // ------------------------------------------------------------------------------------------------------------
    // logging state: every line is prefixed with [P2X][config][step][n/total][sitemap path] so a single item
    // (or step) can be grepped out of the updater output or the cms log
    // ------------------------------------------------------------------------------------------------------------

    String logConfig = "-"
    String logStep = "init"
    String logItem = null
    int logIndex = 0
    int logTotal = 0
    Map<String, Integer> outcomeCounts = new TreeMap<>()
    List<Map<String, Object>> outcomeRecords = new ArrayList<>()

    // ============================================================================================================
    // updater lifecycle
    // ============================================================================================================

    void initialize(Session session) {
        this.session = session
        this.dryRun = visitorContext != null && visitorContext.isDryRun()
        logLevel = parseLogLevel(strParam("logLevel", "item"))

        hstConfigurations = listParam("hstConfigurations", null)
        if (hstConfigurations.isEmpty()) {
            hstConfigurations = listParam("hstConfiguration", [])   // legacy single-value name
        }
        steps = listParam("steps", ["analyze"]).collect { it.toLowerCase() }
        docType = strParam("defaultXpageDocType", null)
        primaryDocumentMode = strParam("primaryDocumentMode", "connector")
        xPageFolder = strParam("xPageFolder", "xpages")
        documentTemplateCategory = strParam("documentTemplateCategory", "new-xpage-document")
        folderTypes = listParam("folderTypes", ["new-xpage-document", "new-xpage-folder"])
        contentRootPath = strParam("contentRootPath", null)
        templateMapping = mapParam("templateMapping")
        fallbackXPageTemplate = strParam("fallbackXPageTemplate", null)
        unmatchedContainers = strParam("unmatchedContainers", "skip-page")
        existingDocument = strParam("existingDocument", "skip")
        publish = boolParam("publish", boolParam("publishPages", true))
        publishPendingChanges = boolParam("publishPendingChanges", false)
        titleProperty = strParam("titleProperty", null)
        setDisplayName = boolParam("setDisplayName", true)
        parseInheritedHstConfiguration = boolParam("parseInheritedHstConfiguration", false)
        overwriteXPageTemplates = boolParam("overwriteXPageTemplates", false)
        generateMissingTemplates = boolParam("generateMissingTemplates", false)
        discardPreviewConfiguration = boolParam("discardPreviewConfiguration", false)
        cleanupUnpublished = boolParam("cleanupUnpublished", false)
        includePaths = listParam("includePaths", []).collect { StringUtils.strip(it, "/") }
        limit = intParam("limit", 0)
        progressEvery = intParam("progressEvery", 25)
        reportToRepository = boolParam("reportToRepository", false)

        List<String> problems = []
        if (hstConfigurations.isEmpty()) problems << "hstConfigurations is empty"
        steps.each { if (!STEP_ORDER.contains(it)) problems << "unknown step '" + it + "' (valid: " + STEP_ORDER + ")" }
        if (!["connector", "mixin"].contains(primaryDocumentMode)) problems << "primaryDocumentMode must be connector|mixin"
        if (!["skip-page", "merge-into-primary"].contains(unmatchedContainers)) problems << "unmatchedContainers must be skip-page|merge-into-primary"
        if (!["skip", "reapply", "recreate"].contains(existingDocument)) problems << "existingDocument must be skip|reapply|recreate"
        if (steps.contains("convert") && StringUtils.isBlank(docType)) problems << "defaultXpageDocType is required for the convert step"
        // run steps in their canonical order, whatever order they were given in
        steps = STEP_ORDER.findAll { steps.contains(it) }
        if (dryRun && steps.any { MUTATING_STEPS.contains(it) }) {
            info("DRY RUN is enabled: mutating steps " + steps.findAll { MUTATING_STEPS.contains(it) } + " are replaced by 'analyze'")
            steps = ["analyze"]
        }

        info("==================================================================================")
        info("Landing page -> XPage converter v" + VERSION)
        info("effective parameters:")
        effectiveParameters().each { k, v -> info("  " + k + " = " + v) }
        if (!problems.isEmpty()) {
            problems.each { log.error("[P2X] invalid parameter: " + it) }
            throw new IllegalArgumentException("[P2X] invalid parameters: " + problems.join("; "))
        }
        if (steps.any { MUTATING_STEPS.contains(it) }) {
            info("**** WRITES ENABLED for steps " + steps.findAll { MUTATING_STEPS.contains(it) } + " ****")
        } else {
            info("read-only run (no repository changes will be made)")
        }
        info("==================================================================================")
    }

    @Override
    boolean doUpdate(final Node node) throws RepositoryException {
        if (!node.isNodeType("hst:configurations")) {
            trace("ignoring visited node " + node.getPath() + " (not hst:configurations; use the XPath query /jcr:root/<hstRoot>/hst:configurations)")
            return false
        }
        boolean changed = false
        for (String configName : hstConfigurations) {
            if (!node.hasNode(configName)) {
                warn("hst configuration '" + configName + "' not found under " + node.getPath() + " - skipped")
                continue
            }
            Node configNode = node.getNode(configName)
            if (!configNode.isNodeType("hst:configuration") || !visitedConfigurations.add(configNode.getPath())) {
                continue
            }
            changed |= processConfiguration(configNode)
        }
        return changed
    }

    @Override
    boolean undoUpdate(final Node node) throws RepositoryException, UnsupportedOperationException {
        throw new UnsupportedOperationException("undo is not supported, restore from a backup / re-run with a fresh repository")
    }

    @Override
    void destroy() {
        logConfig = "-"; logStep = "end"; logItem = null; logTotal = 0
        info("finished. outcome totals over all configurations/steps:")
        outcomeCounts.each { k, v -> info(String.format("  %-60s %5d", k, v)) }
    }

    // ============================================================================================================
    // per configuration
    // ============================================================================================================

    boolean processConfiguration(Node configNode) {
        logConfig = configNode.getName()
        boolean changed = false
        for (String step : steps) {
            logStep = step; logItem = null; logIndex = 0; logTotal = 0
            outcomeRecords = new ArrayList<>()
            long start = System.currentTimeMillis()
            info("---------------------------------------- STEP " + step.toUpperCase() + " START ----------------------------------------")
            try {
                if (MUTATING_STEPS.contains(step) && !checkPreviewConfiguration(configNode)) {
                    outcome(null, "ABORTED", "ABORT_PREVIEW_EXISTS", "remove/publish the -preview configuration first or set discardPreviewConfiguration=true")
                    break
                }
                Ctx ctx = buildContext(configNode)
                switch (step) {
                    case "analyze": stepAnalyze(ctx); break
                    case "templates": changed |= stepTemplates(ctx); break
                    case "sitemap": changed |= stepSitemap(ctx); break
                    case "convert": changed |= stepConvert(ctx); break
                    case "cleanup": changed |= stepCleanup(ctx); break
                }
            } catch (Exception e) {
                error("step '" + step + "' aborted", e)
                safeRefresh()
                outcome(null, "FAILED", "FAIL_STEP", e.getClass().getSimpleName() + ": " + e.getMessage())
                logItem = null
                writeReport(configNode)
                break
            }
            logItem = null; logTotal = 0
            writeReport(configNode)
            info("---------------------------------------- STEP " + step.toUpperCase() + " END (" + (System.currentTimeMillis() - start) + " ms) ----------------------------------------")
        }
        logStep = "-"
        return changed
    }

    boolean checkPreviewConfiguration(Node configNode) {
        Node configurations = configNode.getParent()
        String previewName = configNode.getName() + "-preview"
        if (!configurations.hasNode(previewName)) {
            return true
        }
        Node preview = configurations.getNode(previewName)
        if (!discardPreviewConfiguration) {
            log.error(prefix() + "preview configuration " + preview.getPath() + " exists. It may contain unpublished channel changes " +
                    "made by editors. Publish or discard them in the Experience manager, or set discardPreviewConfiguration=true to delete it.")
            return false
        }
        warn("discardPreviewConfiguration=true: REMOVING " + preview.getPath() + " (unpublished channel changes are lost)")
        trace("removed preview configuration content:\n" + dump(preview, 3))
        preview.remove()
        session.save()
        return true
    }

    // ============================================================================================================
    // model
    // ============================================================================================================

    /** everything we know about one hst:configuration */
    static class Ctx {
        Node configNode
        String name
        Node hstRoot
        List<Node> inherited = []                 // configurations this one inherits from (resolved, ordered)
        List<Node> workspaceInheritors = []       // configurations that inherit THIS configuration's workspace
        Node contentRoot
        String contentRootProblem
        Map<String, Layout> layouts = new LinkedHashMap<>()
        List<Item> items = []                     // all workspace sitemap items, depth first
        Map<String, Page> pagesByPath = [:]
    }

    static class Container {
        String relPath            // path relative to the page / layout root, e.g. "main" or "main/content/left"
        Node node                 // the real container node (resolved when it came from a reference)
        String identifier
        String referencePath      // set when the page used an hst:containercomponentreference
        String xtype
        int itemCount
        String toString() { relPath + (identifier ? "(id=" + identifier + ")" : "") + (referencePath ? "->ref:" + referencePath : "") }
    }

    static class Page {
        String id                 // hst:componentconfigurationid as used on the sitemap item
        Node node
        String path
        String owner              // workspace | configuration | inherited:<name>
        String referenceComponent
        List<Container> containers = []
        List<String> problems = []
        String signature
        List<String> contentBeanComponents = []   // components without an explicit "document" parameter
    }

    static class Layout {
        String name
        Node node
        String owner              // own | inherited:<name>
        String referenceComponent
        List<Container> containers = []
        String signature
    }

    static class Item {
        Node node
        String path
        String relPath            // relative to the workspace sitemap, e.g. foolv1/barlv2
        String name
        int depth
        boolean wildcard
        String ccid
        String relativeContentPath
        Page page
        boolean hasConvertibleDescendant
        List<Item> children = []
        // planning
        String action             // CONVERT | SKIP | ALREADY_CONVERTED
        String reason
        String detail
        Match match
        String targetRelPath      // relative to the content root
        String targetState        // new | existing-xpage | conflict:<why>
    }

    static class Match {
        Layout layout
        String how                // mapping | scored | fallback | merged
        Map<Container, Container> mapping = new LinkedHashMap<>()   // landing container -> layout container
        List<Container> unmatched = []
        List<String> conflicts = []
        Map<Container, Container> conflictTargets = new LinkedHashMap<>()
        int mapped
        boolean signatureEqual
        boolean sameReference
        String toString() { layout?.name + "[" + how + ", mapped=" + mapped + ", unmatched=" + unmatched.size() + ", conflicts=" + conflicts.size() + "]" }
    }

    Ctx buildContext(Node configNode) {
        Ctx ctx = new Ctx()
        ctx.configNode = configNode
        ctx.name = configNode.getName()
        ctx.hstRoot = configNode.getParent().getParent()
        resolveInheritance(ctx)
        resolveContentRoot(ctx)
        loadLayouts(ctx)
        if (configNode.hasNode("hst:workspace/hst:sitemap")) {
            Node sitemap = configNode.getNode("hst:workspace/hst:sitemap")
            for (NodeIterator it = sitemap.getNodes(); it.hasNext();) {
                ctx.items.addAll(loadItem(ctx, it.nextNode(), sitemap.getPath(), null))
            }
        } else {
            warn("no hst:workspace/hst:sitemap in " + configNode.getPath() + ": there are no landing pages to convert")
        }
        ctx.items.findAll { it.relPath.indexOf('/') < 0 }.each { markConvertibleDescendants(it) }
        trace("model: " + ctx.items.size() + " workspace sitemap items, " + ctx.pagesByPath.size() + " pages, " + ctx.layouts.size() + " layouts")
        return ctx
    }

    void resolveInheritance(Ctx ctx) {
        Node configurations = ctx.configNode.getParent()
        for (String value : multiValue(ctx.configNode, "hst:inheritsfrom")) {
            Node target = resolveConfigurationReference(configurations, value)
            if (target == null) {
                warn("hst:inheritsfrom '" + value + "' could not be resolved to a configuration")
            } else if (!ctx.inherited.any { it.getPath() == target.getPath() }) {
                ctx.inherited << target
            }
        }
        for (NodeIterator it = configurations.getNodes(); it.hasNext();) {
            Node other = it.nextNode()
            if (other.getPath() == ctx.configNode.getPath()) continue
            if (multiValue(other, "hst:inheritsfrom").any { StringUtils.removeStart(it, "../") == ctx.name + "/hst:workspace" }) {
                ctx.workspaceInheritors << other
            }
        }
        trace("inherits from " + ctx.inherited.collect { it.getName() } + ", workspace inherited by " + ctx.workspaceInheritors.collect { it.getName() })
    }

    /** "../common", "../common/hst:workspace", "common" -> the configuration node "common" */
    Node resolveConfigurationReference(Node configurations, String value) {
        String cleaned = value.replaceAll('^(\\./|\\.\\./)+', "").replaceAll('^/+', "")
        String configName = cleaned.contains("/") ? cleaned.substring(0, cleaned.indexOf('/')) : cleaned
        if (StringUtils.isNotBlank(configName) && configurations.hasNode(configName)) {
            Node candidate = configurations.getNode(configName)
            return candidate.isNodeType("hst:configuration") ? candidate : null
        }
        return null
    }

    void resolveContentRoot(Ctx ctx) {
        if (StringUtils.isNotBlank(contentRootPath)) {
            if (session.nodeExists(contentRootPath)) {
                ctx.contentRoot = session.getNode(contentRootPath)
            } else {
                ctx.contentRootProblem = "contentRootPath " + contentRootPath + " does not exist"
            }
            return
        }
        List<String> considered = []
        if (ctx.hstRoot.hasNode("hst:sites")) {
            for (NodeIterator it = ctx.hstRoot.getNode("hst:sites").getNodes(); it.hasNext();) {
                Node site = it.nextNode()
                String configPath = site.hasProperty("hst:configurationpath") ? site.getProperty("hst:configurationpath").getString() : null
                boolean matches = configPath != null ? configPath.endsWith("/" + ctx.name) : site.getName() == ctx.name
                considered << site.getName() + (configPath != null ? "(configurationpath=" + configPath + ")" : "")
                if (matches && site.hasProperty("hst:content")) {
                    Property content = site.getProperty("hst:content")
                    String contentPath = content.getString()
                    if (content.getType() == PropertyType.STRING && contentPath.startsWith("/") && session.nodeExists(contentPath)) {
                        ctx.contentRoot = session.getNode(contentPath)
                    } else {
                        try { ctx.contentRoot = content.getNode() } catch (RepositoryException ignored) { }
                    }
                    if (ctx.contentRoot != null) {
                        trace("content root resolved from site " + site.getPath() + ": " + ctx.contentRoot.getPath())
                        return
                    }
                    ctx.contentRootProblem = "site " + site.getPath() + " has hst:content=" + contentPath + " which does not exist"
                    return
                }
            }
        }
        ctx.contentRootProblem = "no hst:site found for configuration '" + ctx.name + "' (sites considered: " + considered + "). Set the contentRootPath parameter."
    }

    void loadLayouts(Ctx ctx) {
        ctx.layouts.clear()
        List<Node> sources = [ctx.configNode] + ctx.inherited
        for (Node source : sources) {
            if (!source.hasNode("hst:xpages")) continue
            String owner = source.getPath() == ctx.configNode.getPath() ? "own" : "inherited:" + source.getName()
            for (NodeIterator it = source.getNode("hst:xpages").getNodes(); it.hasNext();) {
                Node xpage = it.nextNode()
                if (!xpage.isNodeType("hst:xpage") || ctx.layouts.containsKey(xpage.getName())) continue
                Layout layout = new Layout(name: xpage.getName(), node: xpage, owner: owner,
                        referenceComponent: stringProp(xpage, "hst:referencecomponent"))
                collectStructure(ctx, xpage, "", layout.containers, null, null)
                layout.signature = signature(xpage)
                ctx.layouts.put(layout.name, layout)
            }
        }
    }

    List<Item> loadItem(Ctx ctx, Node node, String sitemapPath, Item parent) {
        Item item = new Item(node: node, path: node.getPath(), name: node.getName())
        item.relPath = node.getPath().substring(sitemapPath.length() + 1)
        item.depth = item.relPath.split("/").length
        item.wildcard = WILDCARD_MARKERS.any { item.name.contains(it) }
        item.ccid = stringProp(node, "hst:componentconfigurationid")
        String rcp = stringProp(node, "hst:relativecontentpath")
        // absolute relative content paths ("/foo/bar") are tolerated by the HST but break hasNode(); normalize in memory only
        item.relativeContentPath = rcp != null ? StringUtils.strip(rcp, "/") : null
        if (item.ccid != null) {
            item.page = resolvePage(ctx, item.ccid)
        }
        List<Item> all = [item]
        if (parent != null) parent.children << item
        for (NodeIterator it = node.getNodes(); it.hasNext();) {
            all.addAll(loadItem(ctx, it.nextNode(), sitemapPath, item))
        }
        return all
    }

    boolean markConvertibleDescendants(Item item) {
        boolean any = false
        for (Item child : item.children) {
            boolean childConvertible = markConvertibleDescendants(child)
            boolean repointed = child.relativeContentPath != null && child.relativeContentPath.startsWith(xPageFolder + "/")
            any |= childConvertible || (!child.wildcard && (child.page != null || repointed))
        }
        item.hasConvertibleDescendant = any
        return any
    }

    Page resolvePage(Ctx ctx, String ccid) {
        List<Object[]> candidates = []
        candidates << ([ctx.configNode, "hst:workspace/" + ccid, "workspace"] as Object[])
        candidates << ([ctx.configNode, ccid, "configuration"] as Object[])
        ctx.inherited.each { inh ->
            candidates << ([inh, "hst:workspace/" + ccid, "inherited:" + inh.getName()] as Object[])
            candidates << ([inh, ccid, "inherited:" + inh.getName()] as Object[])
        }
        for (Object[] c : candidates) {
            Node base = (Node) c[0]
            String rel = (String) c[1]
            if (safeHasNode(base, rel)) {
                Node pageNode = base.getNode(rel)
                Page cached = ctx.pagesByPath.get(pageNode.getPath())
                if (cached != null) return cached
                Page page = new Page(id: ccid, node: pageNode, path: pageNode.getPath(), owner: (String) c[2],
                        referenceComponent: stringProp(pageNode, "hst:referencecomponent"))
                collectStructure(ctx, pageNode, "", page.containers, page.problems, page.contentBeanComponents)
                page.signature = signature(pageNode)
                ctx.pagesByPath.put(page.path, page)
                return page
            }
        }
        return null
    }

    /** collect the containers of a page or layout (recursing through static components, resolving container references) */
    void collectStructure(Ctx ctx, Node node, String base, List<Container> containers, List<String> problems, List<String> contentBean) {
        for (NodeIterator it = node.getNodes(); it.hasNext();) {
            Node child = it.nextNode()
            String rel = base.isEmpty() ? child.getName() : base + "/" + child.getName()
            if (child.isNodeType("hst:containercomponent")) {
                containers << container(rel, child, null)
                if (contentBean != null) collectContentBeanComponents(child, rel, contentBean)
            } else if (child.isNodeType("hst:containercomponentreference")) {
                String ref = stringProp(child, "hst:referencecomponent")
                Node target = resolveContainerReference(ctx, ref)
                if (target == null) {
                    problems?.add("unresolved container reference " + rel + " -> " + ref)
                    containers << new Container(relPath: rel, referencePath: ref)
                } else {
                    containers << container(rel, target, target.getPath())
                    if (contentBean != null) collectContentBeanComponents(target, rel, contentBean)
                }
            } else if (child.isNodeType("hst:component")) {
                if (contentBean != null && hasComponentClass(child) && !hasDocumentParameter(child)) {
                    contentBean << rel + " (static, " + componentClass(child) + ")"
                }
                collectStructure(ctx, child, rel, containers, problems, contentBean)
            }
        }
    }

    Container container(String rel, Node node, String referencePath) {
        int items = 0
        for (NodeIterator it = node.getNodes(); it.hasNext();) { if (it.nextNode().isNodeType("hst:containeritemcomponent")) items++ }
        return new Container(relPath: rel, node: node, identifier: stringProp(node, "hippo:identifier"), referencePath: referencePath,
                xtype: normalizeXType(stringProp(node, "hst:xtype")), itemCount: items)
    }

    void collectContentBeanComponents(Node container, String rel, List<String> contentBean) {
        for (NodeIterator it = container.getNodes(); it.hasNext();) {
            Node item = it.nextNode()
            if (item.isNodeType("hst:containeritemcomponent") && !hasDocumentParameter(item)) {
                contentBean << rel + "/" + item.getName() + " (" + componentClass(item) + ")"
            }
        }
    }

    Node resolveContainerReference(Ctx ctx, String ref) {
        if (StringUtils.isBlank(ref)) return null
        for (Node cfg : [ctx.configNode] + ctx.inherited) {
            for (String rel : ["hst:workspace/hst:containers/" + ref, "hst:containers/" + ref]) {
                if (safeHasNode(cfg, rel)) return cfg.getNode(rel)
            }
        }
        return null
    }

    /** structural fingerprint used as tie breaker when several layouts can host a page */
    String signature(Node root) {
        List<String> parts = []
        collectSignature(root, "", parts)
        Collections.sort(parts)
        return parts.join("|")
    }

    void collectSignature(Node node, String base, List<String> parts) {
        for (NodeIterator it = node.getNodes(); it.hasNext();) {
            Node child = it.nextNode()
            String rel = base.isEmpty() ? child.getName() : base + "/" + child.getName()
            if (child.isNodeType("hst:containercomponent") || child.isNodeType("hst:containercomponentreference")) {
                parts << rel + ":container"
            } else if (child.isNodeType("hst:component")) {
                parts << rel + ":" + StringUtils.defaultString(stringProp(child, "hst:componentclassname")) + ":" + StringUtils.defaultString(stringProp(child, "hst:template"))
                collectSignature(child, rel, parts)
            }
        }
    }

    // ============================================================================================================
    // planning (shared by analyze / convert / cleanup so they always agree)
    // ============================================================================================================

    void planItem(Ctx ctx, Item item) {
        item.action = "SKIP"; item.reason = null; item.detail = null; item.match = null; item.targetRelPath = null; item.targetState = null
        if (item.ccid == null) {
            if (item.relativeContentPath != null && isXPageHandle(contentNode(ctx, item.relativeContentPath))) {
                item.action = "ALREADY_CONVERTED"; item.reason = "ALREADY_CONVERTED"
                item.detail = "relative content path points to XPage document " + item.relativeContentPath
            } else {
                item.reason = "SKIP_NO_PAGE_REFERENCE"; item.detail = "no hst:componentconfigurationid"
            }
            return
        }
        if (item.wildcard || (item.relativeContentPath != null && item.relativeContentPath.contains('${'))) {
            item.reason = "SKIP_WILDCARD"; item.detail = "wildcard sitemap items cannot become a single document"
            return
        }
        if (!includePaths.isEmpty() && !includePaths.any { item.relPath == it || item.relPath.startsWith(it + "/") }) {
            item.reason = "SKIP_EXCLUDED"; item.detail = "not under includePaths " + includePaths
            return
        }
        if (item.page == null) {
            item.reason = "SKIP_NO_PAGE"
            item.detail = "page '" + item.ccid + "' not found in workspace, configuration" + (ctx.inherited ? " or inherited " + ctx.inherited.collect { it.getName() } : "")
            return
        }
        if (item.relativeContentPath != null && isXPageHandle(contentNode(ctx, item.relativeContentPath))) {
            // the document behind this page already is an XPage (mixin mode run, or converted by hand): converting it
            // again in connector mode would create a second document for the same URL
            item.action = "ALREADY_CONVERTED"; item.reason = "ALREADY_CONVERTED"
            item.detail = "primary document " + item.relativeContentPath + " already is an XPage"
            return
        }
        if (!item.page.problems.isEmpty()) {
            item.reason = "SKIP_PAGE_PROBLEM"; item.detail = item.page.problems.join("; ")
            return
        }
        if (primaryDocumentMode == "mixin" && item.relativeContentPath != null) {
            Node handle = contentNode(ctx, item.relativeContentPath)
            if (handle == null || !handle.isNodeType("hippo:handle")) {
                item.reason = "SKIP_PRIMARY_DOCUMENT_MISSING"
                item.detail = "mixin mode: " + item.relativeContentPath + (handle == null ? " does not exist" : " is not a document handle (" + handle.getPrimaryNodeType().getName() + ")")
                return
            }
        }
        item.match = matchLayout(ctx, item)
        if (item.match == null || item.match.layout == null) {
            item.reason = item.reason ?: "SKIP_NO_TEMPLATE"
            return
        }
        if (!item.match.unmatched.isEmpty() || !item.match.conflicts.isEmpty()) {
            item.reason = "SKIP_UNMATCHED_CONTAINER"
            item.detail = "layout " + item.match.layout.name + ": unmatched=" + item.match.unmatched + " conflicts=" + item.match.conflicts +
                    " (set unmatchedContainers=merge-into-primary, a templateMapping, or generateMissingTemplates=true)"
            return
        }
        item.targetRelPath = targetRelPath(ctx, item)
        item.targetState = targetState(ctx, item)
        if (item.targetState.startsWith("conflict")) {
            item.reason = "SKIP_TARGET_CONFLICT"; item.detail = item.targetState
            return
        }
        if (item.targetState == "existing-xpage" && existingDocument == "skip") {
            item.action = "SKIP"; item.reason = "SKIP_EXISTING_DOCUMENT"
            item.detail = item.targetRelPath + " already is an XPage (existingDocument=skip)"
            return
        }
        item.action = "CONVERT"; item.reason = "CONVERT"
    }

    Match matchLayout(Ctx ctx, Item item) {
        Page page = item.page
        String pageName = page.id.contains("/") ? page.id.substring(page.id.lastIndexOf('/') + 1) : page.id
        String mapped = templateMapping.get(item.relPath) ?: templateMapping.get(page.id) ?: templateMapping.get(pageName)
        if (mapped != null) {
            Layout layout = ctx.layouts.get(mapped)
            if (layout == null) {
                item.reason = "SKIP_MAPPED_TEMPLATE_MISSING"; item.detail = "templateMapping points to unknown layout '" + mapped + "'"
                return null
            }
            Match m = mapContainers(page, layout)
            m.how = "mapping"
            return applyUnmatchedPolicy(m)
        }
        if (ctx.layouts.isEmpty()) {
            item.reason = "SKIP_NO_TEMPLATE"; item.detail = "the configuration has no hst:xpages layouts (run the 'templates' step)"
            return null
        }
        List<Match> candidates = ctx.layouts.values().collect { mapContainers(page, it) }
        candidates.sort { Match a, Match b ->
            int va = (a.unmatched.isEmpty() && a.conflicts.isEmpty()) ? 1 : 0
            int vb = (b.unmatched.isEmpty() && b.conflicts.isEmpty()) ? 1 : 0
            if (va != vb) return vb <=> va
            if (a.mapped != b.mapped) return b.mapped <=> a.mapped
            if (a.signatureEqual != b.signatureEqual) return (b.signatureEqual ? 1 : 0) <=> (a.signatureEqual ? 1 : 0)
            if (a.sameReference != b.sameReference) return (b.sameReference ? 1 : 0) <=> (a.sameReference ? 1 : 0)
            return a.layout.name <=> b.layout.name
        }
        trace("layout candidates for page " + page.path + ":\n    " + candidates.collect { describeMatch(it) }.join("\n    "))
        Match best = candidates.first()
        if (best.unmatched.isEmpty() && best.conflicts.isEmpty()) {
            best.how = "scored"
            return best
        }
        if (fallbackXPageTemplate != null) {
            Layout fallback = ctx.layouts.get(fallbackXPageTemplate)
            if (fallback == null) {
                item.reason = "SKIP_MAPPED_TEMPLATE_MISSING"; item.detail = "fallbackXPageTemplate '" + fallbackXPageTemplate + "' does not exist"
                return null
            }
            Match m = mapContainers(page, fallback)
            m.how = "fallback"
            return applyUnmatchedPolicy(m)
        }
        best.how = "scored"
        return applyUnmatchedPolicy(best)
    }

    /** map every container of the landing page onto a container of the layout: identifier, then path, then unique name */
    Match mapContainers(Page page, Layout layout) {
        Match m = new Match(layout: layout)
        Map<Container, Container> reverse = new HashMap<>()
        for (Container landing : page.containers) {
            Container target = null
            if (landing.identifier != null) {
                List<Container> byId = layout.containers.findAll { it.identifier == landing.identifier }
                if (byId.size() == 1) target = byId.first()
            }
            if (target == null) {
                target = layout.containers.find { it.relPath == landing.relPath }
            }
            if (target == null) {
                String leaf = leafName(landing.relPath)
                List<Container> byName = layout.containers.findAll { leafName(it.relPath) == leaf }
                if (byName.size() == 1) target = byName.first()
            }
            if (target == null) {
                m.unmatched << landing
            } else if (reverse.containsKey(target)) {
                m.conflicts << (landing.relPath + " and " + reverse.get(target).relPath + " both map to " + target.relPath)
                m.conflictTargets.put(landing, target)
            } else {
                reverse.put(target, landing)
                m.mapping.put(landing, target)
            }
        }
        m.mapped = m.mapping.size()
        m.signatureEqual = page.signature == layout.signature
        m.sameReference = page.referenceComponent == layout.referenceComponent
        return m
    }

    Match applyUnmatchedPolicy(Match m) {
        if ((m.unmatched.isEmpty() && m.conflicts.isEmpty()) || unmatchedContainers != "merge-into-primary") {
            return m
        }
        if (m.layout.containers.isEmpty()) {
            return m   // nothing to merge into, stays unmatched -> SKIP_UNMATCHED_CONTAINER
        }
        Container primary = m.layout.containers.first()
        m.unmatched.each { m.mapping.put(it, primary) }
        // a conflict means two landing containers want the same layout container; with merge both go there
        m.conflictTargets.each { landing, target -> m.mapping.put(landing, target) }
        m.conflicts.clear()
        m.conflictTargets.clear()
        m.unmatched.clear()
        m.how = m.how + "+merged"
        return m
    }

    String describeMatch(Match m) {
        return String.format("%-30s valid=%-5s mapped=%d unmatched=%s conflicts=%s signatureEqual=%s sameReference=%s",
                m.layout.name, m.unmatched.isEmpty() && m.conflicts.isEmpty(), m.mapped, m.unmatched.collect { it.relPath },
                m.conflicts, m.signatureEqual, m.sameReference)
    }

    /** deterministic target: <xPageFolder>/<sitemap path>; items with convertible children become <path>/index */
    String targetRelPath(Ctx ctx, Item item) {
        if (primaryDocumentMode == "mixin" && item.relativeContentPath != null) {
            return item.relativeContentPath
        }
        List<String> segments = item.relPath.split("/").collect { NodeNameCodec.encode(it, true) }
        if (item.hasConvertibleDescendant) {
            segments << "index"
        }
        return xPageFolder + "/" + segments.join("/")
    }

    String targetState(Ctx ctx, Item item) {
        if (ctx.contentRoot == null) return "new"
        String[] segments = item.targetRelPath.split("/")
        Node current = ctx.contentRoot
        for (int i = 0; i < segments.length - 1; i++) {
            if (!current.hasNode(segments[i])) return "new"
            current = current.getNode(segments[i])
            if (!current.isNodeType("hippostd:folder")) {
                return "conflict: " + current.getPath() + " exists but is a " + current.getPrimaryNodeType().getName() + ", not a folder"
            }
        }
        String leaf = segments[segments.length - 1]
        if (!current.hasNode(leaf)) return "new"
        Node existing = current.getNode(leaf)
        if (!existing.isNodeType("hippo:handle")) {
            return "conflict: " + existing.getPath() + " exists and is a " + existing.getPrimaryNodeType().getName()
        }
        if (isXPageHandle(existing)) return "existing-xpage"
        if (primaryDocumentMode == "mixin" && item.relativeContentPath != null) return "existing-document"
        return "conflict: " + existing.getPath() + " is an existing document that is not an XPage (existingDocument=recreate would delete it)"
    }

    // ============================================================================================================
    // STEP analyze
    // ============================================================================================================

    void stepAnalyze(Ctx ctx) {
        info("configuration      : " + ctx.configNode.getPath())
        info("inherits from      : " + (ctx.inherited ? ctx.inherited.collect { it.getPath() } : "-"))
        info("content root       : " + (ctx.contentRoot != null ? ctx.contentRoot.getPath() : "UNRESOLVED - " + ctx.contentRootProblem))
        if (ctx.contentRoot != null) {
            String folderPath = ctx.contentRoot.getPath() + "/" + xPageFolder
            if (!session.nodeExists(folderPath)) {
                info("xpage folder       : " + folderPath + " (will be created)")
            } else {
                Node folder = session.getNode(folderPath)
                info("xpage folder       : " + folderPath + " exists, type=" + folder.getPrimaryNodeType().getName() +
                        ", xpagefolder=" + folder.isNodeType("hippostd:xpagefolder") + ", foldertype=" + multiValue(folder, "hippostd:foldertype"))
                if (!folder.isNodeType("hippostd:folder")) warn("xpage folder " + folderPath + " is not a hippostd:folder: convert will FAIL")
            }
        }
        checkDocumentType()
        info("prototype pages    : " + prototypeSources(ctx).collect { p -> p.getPath() })
        info("xpage layouts      : " + (ctx.layouts ? ctx.layouts.values().collect { it.name + "(" + it.owner + ", containers=" + it.containers + ")" } : "NONE"))
        if (ctx.configNode.getParent().hasNode(ctx.name + "-preview")) {
            warn("preview configuration " + ctx.name + "-preview exists: mutating steps will abort unless discardPreviewConfiguration=true")
        }

        List<Item> items = ctx.items
        logTotal = items.size()
        List<String> table = []
        items.eachWithIndex { Item item, int i ->
            logIndex = i + 1; logItem = item.relPath
            planItem(ctx, item)
            logItemPlan(ctx, item)
            outcome(item.path, item.action == "CONVERT" ? "WOULD_CONVERT" : (item.action == "SKIP" ? "SKIPPED" : item.action), item.reason, item.detail, planRecord(item))
            table << String.format("%-45s | %-40s | %-24s | %-60s | %s", StringUtils.abbreviate(item.relPath, 45),
                    StringUtils.abbreviate(item.page != null ? item.page.id + "@" + item.page.owner : StringUtils.defaultString(item.ccid, "-"), 40),
                    StringUtils.abbreviate(item.match?.layout?.name ?: "-", 24),
                    StringUtils.abbreviate(StringUtils.defaultString(item.targetRelPath, "-"), 60), item.reason)
        }
        logItem = null; logTotal = 0
        info("analysis table:")
        info(String.format("%-45s | %-40s | %-24s | %-60s | %s", "SITEMAP ITEM", "PAGE@OWNER", "LAYOUT", "TARGET DOCUMENT", "RESULT"))
        table.each { info(it) }
        List<Item> converting = items.findAll { it.action == "CONVERT" }
        Map<String, List<Item>> byPage = converting.groupBy { it.page.path }
        byPage.findAll { it.value.size() > 1 }.each { path, shared ->
            info("shared page " + path + " is used by " + shared.size() + " sitemap items " + shared.collect { it.relPath } +
                    ": each gets its own document, the page is removed by 'cleanup' only after all of them are converted")
        }
        if (generateMissingTemplates) {
            Map<String, List<Item>> groups = missingTemplateGroups(ctx)
            groups.each { sig, its -> info("templates step would GENERATE a layout from page " + its.first().page.path + " for " + its.collect { it.relPath }) }
        }
    }

    void logItemPlan(Ctx ctx, Item item) {
        if (item.reason == "SKIP_NO_PAGE_REFERENCE" || item.reason == "SKIP_EXCLUDED") {
            trace("plan: " + item.reason + " - " + item.detail)
            return
        }
        itemLog("sitemap item " + item.path + (item.relativeContentPath ? " relativecontentpath=" + item.relativeContentPath : ""))
        if (item.page != null) {
            itemLog("  page      : " + item.page.path + " (owner=" + item.page.owner + ", reference=" + item.page.referenceComponent + ")")
            itemLog("  containers: " + (item.page.containers ? item.page.containers.collect { it.toString() + "[" + it.itemCount + " items]" } : "none"))
            if (item.relativeContentPath != null && primaryDocumentMode == "connector" && !item.page.contentBeanComponents.isEmpty()) {
                warn("  CONTENT BEAN: this page is powered by document '" + item.relativeContentPath + "'. After conversion the content bean is the " +
                        "XPage document, so these components (no explicit 'document' parameter) may lose their content: " + item.page.contentBeanComponents)
            }
            if (item.relativeContentPath != null && contentNode(ctx, item.relativeContentPath) == null) {
                warn("  relative content path " + item.relativeContentPath + " does not exist under " + ctx.contentRoot?.getPath())
            }
        }
        if (item.match != null && item.match.layout != null) {
            itemLog("  layout    : " + item.match.toString())
            item.match.mapping.each { landing, layout -> itemLog("    container " + landing.relPath + " -> layout " + layout.relPath + " (id=" + layout.identifier + ")") }
            item.match.unmatched.each { itemLog("    container " + it.relPath + " -> UNMATCHED") }
            item.match.conflicts.each { itemLog("    CONFLICT " + it) }
        }
        if (item.targetRelPath != null) {
            itemLog("  target    : " + item.targetRelPath + " (" + item.targetState + ")")
        }
        itemLog("  result    : " + item.reason + (item.detail ? " - " + item.detail : ""))
    }

    void checkDocumentType() {
        if (StringUtils.isBlank(docType)) {
            warn("defaultXpageDocType is not set (required for convert)")
            return
        }
        boolean typeExists = session.getWorkspace().getNodeTypeManager().hasNodeType(docType)
        String queryPath = "/hippo:configuration/hippo:queries/hippo:templates/" + documentTemplateCategory
        info("document type      : " + docType + (typeExists ? "" : " (NOT REGISTERED)") + ", template query " + documentTemplateCategory +
                (session.nodeExists(queryPath) ? "" : " (MISSING: " + queryPath + ")") + ", mode=" + primaryDocumentMode)
        if (!typeExists) warn("document type " + docType + " is not registered: install the connector document type (see README) or change defaultXpageDocType")
        if (!session.nodeExists(queryPath)) warn("template query " + queryPath + " is missing: documents cannot be created with category " + documentTemplateCategory)
        folderTypes.each { ft ->
            if (!session.nodeExists("/hippo:configuration/hippo:queries/hippo:templates/" + ft)) warn("folder type template query '" + ft + "' does not exist")
        }
    }

    // ============================================================================================================
    // STEP templates
    // ============================================================================================================

    List<Node> prototypeSources(Ctx ctx) {
        List<Node> sources = []
        if (ctx.configNode.hasNode("hst:prototypepages")) sources << ctx.configNode.getNode("hst:prototypepages")
        if (parseInheritedHstConfiguration) {
            ctx.inherited.each { if (it.hasNode("hst:prototypepages")) sources << it.getNode("hst:prototypepages") }
        }
        return sources
    }

    boolean stepTemplates(Ctx ctx) {
        Node xpages = ctx.configNode.hasNode("hst:xpages") ? ctx.configNode.getNode("hst:xpages") : null
        if (xpages == null) {
            xpages = ctx.configNode.addNode("hst:xpages", "hst:xpages")
            info("created " + xpages.getPath())
        }
        Set<String> seen = new HashSet<>()
        List<Node> prototypes = []
        prototypeSources(ctx).each { src ->
            for (NodeIterator it = src.getNodes(); it.hasNext();) {
                Node proto = it.nextNode()
                if (proto.isNodeType("hst:component") && seen.add(proto.getName())) prototypes << proto
            }
        }
        logTotal = prototypes.size()
        prototypes.eachWithIndex { Node proto, int i ->
            logIndex = i + 1; logItem = "prototype:" + proto.getName()
            try {
                String name = proto.getName()
                if (xpages.hasNode(name)) {
                    if (!overwriteXPageTemplates) {
                        outcome(proto.getPath(), "SKIPPED", "SKIP_LAYOUT_EXISTS", xpages.getNode(name).getPath() + " exists (overwriteXPageTemplates=false)")
                        return
                    }
                    warn("overwriting existing layout " + xpages.getNode(name).getPath() + ":\n" + dump(xpages.getNode(name), 4))
                    xpages.getNode(name).remove()
                } else if (ctx.layouts.containsKey(name) && ctx.layouts.get(name).owner != "own") {
                    outcome(proto.getPath(), "SKIPPED", "SKIP_LAYOUT_INHERITED", "layout '" + name + "' is inherited from " + ctx.layouts.get(name).owner)
                    return
                }
                Node layout = copyTree(proto, xpages, name, "hst:xpage", true)
                String label = stringProp(proto, "hst:displayname")
                if (label != null) layout.setProperty("hst:label", label)
                session.save()
                outcome(proto.getPath(), "UPDATED", "LAYOUT_CREATED", layout.getPath() + (label ? " label='" + label + "'" : ""))
                trace("layout structure:\n" + dump(layout, 6))
            } catch (Exception e) {
                error("failed to convert prototype " + proto.getPath(), e)
                safeRefresh()
                outcome(proto.getPath(), "FAILED", "FAIL_REPOSITORY", e.getMessage())
            }
        }
        if (generateMissingTemplates) {
            logItem = null
            loadLayouts(ctx)
            Map<String, List<Item>> groups = missingTemplateGroups(ctx)
            logTotal = groups.size(); logIndex = 0
            groups.each { sig, items ->
                logIndex++
                Page page = items.first().page
                String name = "p2x-" + NodeNameCodec.encode(leafName(page.path), true)
                logItem = "generated:" + name
                try {
                    if (xpages.hasNode(name)) {
                        outcome(page.path, "SKIPPED", "SKIP_LAYOUT_EXISTS", xpages.getNode(name).getPath() + " exists")
                        return
                    }
                    Node layout = copyPageAsLayout(ctx, page, xpages, name)
                    layout.setProperty("hst:label", "Converted: " + leafName(page.path))
                    session.save()
                    outcome(page.path, "UPDATED", "LAYOUT_GENERATED", layout.getPath() + " for " + items.collect { it.relPath })
                    trace("generated layout structure:\n" + dump(layout, 6))
                } catch (Exception e) {
                    error("failed to generate layout from " + page.path, e)
                    safeRefresh()
                    outcome(page.path, "FAILED", "FAIL_REPOSITORY", e.getMessage())
                }
            }
        }
        return true
    }

    /** convertible items that currently have no valid layout, grouped by page structure */
    Map<String, List<Item>> missingTemplateGroups(Ctx ctx) {
        Map<String, List<Item>> groups = new LinkedHashMap<>()
        for (Item item : ctx.items) {
            planItem(ctx, item)
            if (item.reason in ["SKIP_NO_TEMPLATE", "SKIP_UNMATCHED_CONTAINER"] && item.page != null) {
                String key = item.page.signature + "#" + item.page.referenceComponent
                groups.computeIfAbsent(key, { new ArrayList<Item>() }).add(item)
            }
        }
        return groups
    }

    /** a layout derived from a landing page: same static structure, empty containers (references become real containers) */
    Node copyPageAsLayout(Ctx ctx, Page page, Node xpages, String name) {
        Node layout = xpages.addNode(name, "hst:xpage")
        copyProperties(page.node, layout, true)
        copyLayoutChildren(page.node, layout)
        return layout
    }

    void copyLayoutChildren(Node source, Node target) {
        for (NodeIterator it = source.getNodes(); it.hasNext();) {
            Node child = it.nextNode()
            if (child.isNodeType("hst:containercomponent") || child.isNodeType("hst:containercomponentreference")) {
                Node container = target.addNode(child.getName(), "hst:containercomponent")
                String xtype = stringProp(child, "hst:xtype")
                if (xtype != null) container.setProperty("hst:xtype", xtype)
                String label = stringProp(child, "hst:label")
                if (label != null) container.setProperty("hst:label", label)
                // hippo:identifier is generated by the repository for the new container
            } else if (child.isNodeType("hst:component")) {
                Node copy = target.addNode(child.getName(), child.getPrimaryNodeType().getName())
                copyProperties(child, copy, true)
                copyLayoutChildren(child, copy)
            }
        }
    }

    // ============================================================================================================
    // STEP sitemap
    // ============================================================================================================

    boolean stepSitemap(Ctx ctx) {
        Node sitemap
        if (ctx.configNode.hasNode("hst:sitemap")) {
            sitemap = ctx.configNode.getNode("hst:sitemap")
        } else if (ctx.configNode.hasNode("hst:workspace/hst:sitemap")) {
            // creating an own hst:sitemap would hide the inherited one, so use the (merged) workspace sitemap instead
            sitemap = ctx.configNode.getNode("hst:workspace/hst:sitemap")
            info("configuration has no own hst:sitemap (inherited); adding the xpage subtree to the workspace sitemap " + sitemap.getPath())
        } else {
            outcome(null, "SKIPPED", "SKIP_NO_SITEMAP", "no hst:sitemap or hst:workspace/hst:sitemap to add to")
            return false
        }
        String itemName = NodeNameCodec.encode(xPageFolder, true)
        if (sitemap.hasNode(itemName)) {
            outcome(sitemap.getPath() + "/" + itemName, "SKIPPED", "SKIP_SITEMAP_EXISTS", "sitemap item exists, not touching it:\n" + dump(sitemap.getNode(itemName), 4))
            return false
        }
        int depth = 1
        ctx.items.each { planItem(ctx, it); if (it.targetRelPath != null) depth = Math.max(depth, it.targetRelPath.split("/").length - 1) }
        depth = Math.max(depth, 3)
        Node root = sitemap.addNode(itemName, "hst:sitemapitem")
        root.setProperty("hst:relativecontentpath", xPageFolder)
        root.setProperty("hst:hiddeninchannelmanager", true)
        Node current = root
        for (int level = 1; level <= depth; level++) {
            Node index = current.addNode("_index_", "hst:sitemapitem")
            index.setProperty("hst:relativecontentpath", '${parent}/index')
            Node wildcard = current.addNode("_default_", "hst:sitemapitem")
            wildcard.setProperty("hst:relativecontentpath", '${parent}/${' + level + '}')
            current = wildcard
        }
        session.save()
        outcome(root.getPath(), "UPDATED", "SITEMAP_CREATED", "hidden sitemap subtree with depth " + depth + " for documents under " + xPageFolder)
        trace("sitemap subtree:\n" + dump(root, depth + 2))
        return true
    }

    // ============================================================================================================
    // STEP convert
    // ============================================================================================================

    boolean stepConvert(Ctx ctx) {
        if (ctx.contentRoot == null) {
            throw new IllegalStateException("content root unresolved: " + ctx.contentRootProblem)
        }
        if (!session.getWorkspace().getNodeTypeManager().hasNodeType(docType)) {
            throw new IllegalStateException("document type " + docType + " is not registered")
        }
        ctx.items.each { planItem(ctx, it) }
        List<Item> todo = ctx.items.findAll { it.action == "CONVERT" }
        ctx.items.findAll { it.action != "CONVERT" && !(it.reason in ["SKIP_NO_PAGE_REFERENCE", "SKIP_EXCLUDED"]) }.each {
            logItem = it.relPath
            outcome(it.path, "SKIPPED", it.reason, it.detail)
        }
        logItem = null
        if (limit > 0 && todo.size() > limit) {
            info("limit=" + limit + ": converting the first " + limit + " of " + todo.size() + " items")
            todo = todo.subList(0, limit)
        }
        info(todo.size() + " sitemap items to convert")
        logTotal = todo.size()
        long start = System.currentTimeMillis()
        boolean changed = false
        todo.eachWithIndex { Item item, int i ->
            logIndex = i + 1; logItem = item.relPath
            try {
                changed |= convertItem(ctx, item)
            } catch (Exception e) {
                error("conversion failed for " + item.path + " -> " + item.targetRelPath, e)
                safeRefresh()
                outcome(item.path, "FAILED", e instanceof RepositoryException ? "FAIL_REPOSITORY" : "FAIL_WORKFLOW", e.getClass().getSimpleName() + ": " + e.getMessage())
            }
            progress(i + 1, todo.size(), start)
        }
        return changed
    }

    boolean convertItem(Ctx ctx, Item item) {
        itemLog("converting " + item.path + " (page " + item.page.path + ") with layout " + item.match + " -> " + item.targetRelPath + " (" + item.targetState + ")")
        Node handle
        boolean created = false
        String relPath = item.targetRelPath
        Node existing = contentNode(ctx, relPath)

        if (existing != null && item.targetState == "existing-xpage" && existingDocument == "recreate") {
            warn("existingDocument=recreate: deleting existing XPage document " + existing.getPath() + ":\n" + dump(existing, 3))
            existing.remove()
            session.save()
            existing = null
        }
        if (existing != null) {
            handle = existing
            if (isBeingEdited(handle)) {
                outcome(item.path, "SKIPPED", "SKIP_DOCUMENT_IN_EDIT", handle.getPath() + " is being edited by " + editor(handle))
                return false
            }
            itemLog("  using existing document " + handle.getPath())
        } else {
            Node folder = ensureFolders(ctx, relPath.substring(0, relPath.lastIndexOf('/')))
            session.save()
            String docName = relPath.substring(relPath.lastIndexOf('/') + 1)
            JcrTemplateNode template = JcrTemplateNodeConverter.getXPageLaoutAsJcrTemplate(new HstNodeImpl(item.match.layout.node, (HstNode) null))
            FolderWorkflow folderWorkflow = (FolderWorkflow) workflow("threepane", folder)
            if (folderWorkflow == null) throw new IllegalStateException("no folder workflow available on " + folder.getPath())
            itemLog("  workflow: add(category=" + documentTemplateCategory + ", type=" + docType + ", name=" + docName + ", layout=" + item.match.layout.name + ") in " + folder.getPath())
            String variantPath = folderWorkflow.add(documentTemplateCategory, docType, docName, template)
            session.refresh(false)
            handle = session.getNode(variantPath).getParent()
            created = true
            itemLog("  created " + handle.getPath() + " (variants " + variants(handle).keySet() + ")")
        }
        // decide before touching the document: did editors leave changes that were never published?
        boolean pendingChanges = !created && hasPendingChanges(variants(handle))
        // new documents only have a draft, and documents bootstrapped from YAML often only have a published variant:
        // let the workflow create the unpublished variant instead of writing one by hand
        ensureUnpublishedVariant(handle)
        if (primaryDocumentMode == "mixin" && !handle.getParent().isNodeType("hippostd:xpagefolder")) {
            warn("  mixin mode: " + handle.getParent().getPath() + " is not an XPage folder (hippostd:xpagefolder); the Experience manager may not offer this page for editing")
        }

        Map<String, Node> variants = variants(handle)
        Node unpublished = variants.get("unpublished")
        if (unpublished == null) throw new IllegalStateException("document " + handle.getPath() + " has no unpublished variant (variants=" + variants.keySet() + ")")

        if (!unpublished.isNodeType("hst:xpagemixin")) {
            if (!unpublished.canAddMixin("hst:xpagemixin")) {
                throw new IllegalStateException("document type " + unpublished.getPrimaryNodeType().getName() + " does not allow mixin hst:xpagemixin")
            }
            unpublished.addMixin("hst:xpagemixin")
        }
        int copied = applyXPage(unpublished, item)
        Node draft = variants.get("draft")
        if (draft != null) {
            // keep an existing (not edited) draft in sync, otherwise the next edit would bring back the old layout
            if (!draft.isNodeType("hst:xpagemixin") && draft.canAddMixin("hst:xpagemixin")) draft.addMixin("hst:xpagemixin")
            applyXPage(draft, item)
        }
        if (titleProperty != null && StringUtils.isNotBlank(stringProp(item.node, "hst:pagetitle"))) {
            String title = stringProp(item.node, "hst:pagetitle")
            if (created || StringUtils.isBlank(stringProp(unpublished, titleProperty))) {
                unpublished.setProperty(titleProperty, title)
                itemLog("  " + titleProperty + " = '" + title + "'")
            }
        }
        if (created && setDisplayName && StringUtils.isNotBlank(stringProp(item.node, "hst:pagetitle"))) {
            String title = stringProp(item.node, "hst:pagetitle")
            if (!handle.isNodeType("hippo:named") && handle.canAddMixin("hippo:named")) handle.addMixin("hippo:named")
            if (handle.isNodeType("hippo:named")) handle.setProperty("hippo:name", title)
        }
        populateDocumentVariant(unpublished, item, ctx)
        session.save()
        itemLog("  saved unpublished variant (" + copied + " container items copied)")

        String publishInfo = "not published (publish=false)"
        if (publish) {
            if (!created && pendingChanges && !publishPendingChanges) {
                publishInfo = "NOT published: the document had unpublished changes (set publishPendingChanges=true to publish anyway)"
                warn("  " + publishInfo)
            } else {
                DocumentWorkflow documentWorkflow = (DocumentWorkflow) workflow("default", handle)
                Object hint = documentWorkflow.hints().get("publish")
                if (Boolean.TRUE == hint) {
                    documentWorkflow.publish()
                    session.refresh(false)
                    publishInfo = "published"
                    itemLog("  workflow: publish")
                } else {
                    publishInfo = "NOT published: publish not available (hint=" + hint + ")"
                    warn("  " + publishInfo)
                }
            }
        }
        outcome(item.path, "UPDATED", created ? "CONVERTED" : "REAPPLIED",
                handle.getPath() + " layout=" + item.match.layout.name + " items=" + copied + ", " + publishInfo, planRecord(item))
        return true
    }

    void ensureUnpublishedVariant(Node handle) {
        if (variants(handle).containsKey("unpublished")) return
        DocumentWorkflow dw = (DocumentWorkflow) workflow("default", handle)
        Map<String, Serializable> hints = dw.hints()
        itemLog("  " + handle.getPath() + " has variants " + variants(handle).keySet() + ", creating the unpublished variant (obtain=" +
                hints.get("obtainEditableInstance") + ", commit=" + hints.get("commitEditableInstance") + ")")
        if (Boolean.TRUE != hints.get("commitEditableInstance")) {
            dw.obtainEditableInstance()
        }
        dw.commitEditableInstance()
        session.refresh(false)
        itemLog("  workflow: obtain/commitEditableInstance -> variants " + variants(handle).keySet())
        if (!variants(handle).containsKey("unpublished")) {
            throw new IllegalStateException("workflow did not create an unpublished variant for " + handle.getPath() + " (variants=" + variants(handle).keySet() + ")")
        }
    }

    /** write the landing page's container items into the hst:xpage of a document variant; returns the number of items copied */
    int applyXPage(Node variant, Item item) {
        Layout layout = item.match.layout
        Node xpage = variant.hasNode("hst:xpage") ? variant.getNode("hst:xpage") : variant.addNode("hst:xpage", "hst:xpage")
        xpage.setProperty("hst:pageref", layout.name)
        // document containers are flat children of hst:xpage, named after the layout container's hippo:identifier
        Map<Container, Node> documentContainers = new LinkedHashMap<>()
        for (Container lc : layout.containers) {
            if (lc.identifier == null) {
                warn("  layout container " + layout.name + "/" + lc.relPath + " has no hippo:identifier; it cannot be linked to a document container")
                continue
            }
            Node dc = xpage.hasNode(lc.identifier) ? xpage.getNode(lc.identifier) : xpage.addNode(lc.identifier, "hst:containercomponent")
            if (lc.xtype != null && !dc.hasProperty("hst:xtype")) dc.setProperty("hst:xtype", stringProp(lc.node, "hst:xtype"))
            documentContainers.put(lc, dc)
        }
        Set<Node> cleared = new HashSet<>()
        int copied = 0
        item.match.mapping.each { Container landing, Container lc ->
            Node dc = documentContainers.get(lc)
            if (dc == null) return
            if (cleared.add(dc)) {
                // the landing page content replaces the layout's default items
                for (NodeIterator it = dc.getNodes(); it.hasNext();) it.nextNode().remove()
            }
            for (NodeIterator it = landing.node.getNodes(); it.hasNext();) {
                Node source = it.nextNode()
                if (!source.isNodeType("hst:containeritemcomponent")) continue
                if (!keepContainerItem(source, item)) {
                    itemLog("    item " + landing.relPath + "/" + source.getName() + " dropped by keepContainerItem hook")
                    continue
                }
                String name = uniqueChildName(dc, source.getName())
                copyTree(source, dc, name, null, false)
                copied++
                trace("    item " + landing.relPath + "/" + source.getName() + " -> " + dc.getPath() + "/" + name + " (" + componentClass(source) + ")")
            }
            itemLog("    container " + landing.relPath + " (" + landing.itemCount + " items) -> " + layout.name + "/" + lc.relPath + " = " + dc.getPath())
        }
        return copied
    }

    Node ensureFolders(Ctx ctx, String relFolderPath) {
        Node current = ctx.contentRoot
        String[] segments = relFolderPath.split("/")
        for (int i = 0; i < segments.length; i++) {
            String segment = segments[i]
            boolean root = i == 0
            if (current.hasNode(segment)) {
                Node existing = current.getNode(segment)
                if (!existing.isNodeType("hippostd:folder")) {
                    throw new IllegalStateException(existing.getPath() + " exists but is a " + existing.getPrimaryNodeType().getName() + ", not a folder")
                }
                current = existing
                prepareXPageFolder(ctx, current, root, false)
            } else {
                Node parent = current
                current = current.addNode(segment, "hippostd:folder")
                if (current.canAddMixin("mix:versionable")) current.addMixin("mix:versionable")
                if (parent.isNodeType("hippotranslation:translated") && current.canAddMixin("hippotranslation:translated")) {
                    current.addMixin("hippotranslation:translated")
                    current.setProperty("hippotranslation:locale", parent.getProperty("hippotranslation:locale").getString())
                    current.setProperty("hippotranslation:id", UUID.randomUUID().toString())
                }
                prepareXPageFolder(ctx, current, root, true)
                itemLog("  created folder " + current.getPath())
            }
        }
        return current
    }

    void prepareXPageFolder(Ctx ctx, Node folder, boolean root, boolean created) {
        if (!folder.isNodeType("hippostd:xpagefolder")) {
            folder.addMixin("hippostd:xpagefolder")
            itemLog("  added hippostd:xpagefolder to " + folder.getPath())
        }
        if (stringProp(folder, "hippostd:channelid") != ctx.name) {
            folder.setProperty("hippostd:channelid", ctx.name)
        }
        if (root && created && folder.canAddMixin("hippostd:cmxpagefolder")) {
            folder.addMixin("hippostd:cmxpagefolder")
        }
        List<String> current = multiValue(folder, "hippostd:foldertype")
        List<String> merged = new ArrayList<>(current)
        folderTypes.each { if (!merged.contains(it)) merged << it }
        if (!merged.contains(documentTemplateCategory)) merged << documentTemplateCategory
        if (merged != current) {
            folder.setProperty("hippostd:foldertype", merged as String[])
            if (!created) itemLog("  hippostd:foldertype of " + folder.getPath() + ": " + current + " -> " + merged)
        }
    }

    // ============================================================================================================
    // STEP cleanup
    // ============================================================================================================

    boolean stepCleanup(Ctx ctx) {
        if (ctx.contentRoot == null) {
            throw new IllegalStateException("content root unresolved: " + ctx.contentRootProblem)
        }
        List<Item> candidates = []
        for (Item item : ctx.items) {
            if (item.ccid == null || item.page == null || item.wildcard) continue
            if (!includePaths.isEmpty() && !includePaths.any { item.relPath == it || item.relPath.startsWith(it + "/") }) continue
            candidates << item
        }
        logTotal = candidates.size()
        Set<Page> touchedPages = new LinkedHashSet<>()
        boolean changed = false
        candidates.eachWithIndex { Item item, int i ->
            logIndex = i + 1; logItem = item.relPath
            try {
                // the target is computed exactly like convert did (descendants decide on <path>/index); a primary
                // document that already is an XPage (mixin mode) is its own target
                String relPath = item.relativeContentPath != null && isXPageHandle(contentNode(ctx, item.relativeContentPath)) ?
                        item.relativeContentPath : targetRelPath(ctx, item)
                Node handle = contentNode(ctx, relPath)
                if (handle == null || !isXPageHandle(handle)) {
                    outcome(item.path, "SKIPPED", "SKIP_NOT_CONVERTED", "no XPage document at " + relPath + " (run convert first)")
                    return
                }
                Map<String, Node> variants = variants(handle)
                Node live = variants.get("published")
                boolean liveIsXPage = live != null && live.hasNode("hst:xpage") && isLive(live)
                if (!liveIsXPage && !cleanupUnpublished) {
                    outcome(item.path, "SKIPPED", "SKIP_NOT_PUBLISHED", handle.getPath() + " is not published as XPage yet; the live site still needs the old page (cleanupUnpublished=false)")
                    return
                }
                itemLog("cleanup sitemap item before change:\n" + dump(item.node, 0))
                if (item.node.hasProperty("hst:componentconfigurationid")) item.node.getProperty("hst:componentconfigurationid").remove()
                ["hst:componentconfigurationmappingnames", "hst:componentconfigurationmappingvalues"].each {
                    if (item.node.hasProperty(it)) {
                        warn("  removing " + it + " " + multiValue(item.node, it))
                        item.node.getProperty(it).remove()
                    }
                }
                item.node.setProperty("hst:relativecontentpath", relPath)
                session.save()
                touchedPages << item.page
                item.ccid = null
                changed = true
                outcome(item.path, "UPDATED", "SITEMAP_ITEM_REPOINTED", "hst:relativecontentpath=" + relPath + " (was page " + item.page.id + ")")
            } catch (Exception e) {
                error("cleanup failed for " + item.path, e)
                safeRefresh()
                outcome(item.path, "FAILED", "FAIL_REPOSITORY", e.getMessage())
            }
        }
        logItem = null
        logTotal = touchedPages.size(); logIndex = 0
        for (Page page : touchedPages) {
            logIndex++; logItem = "page:" + page.path
            try {
                if (page.owner != "workspace") {
                    outcome(page.path, "SKIPPED", "SKIP_PAGE_NOT_IN_WORKSPACE", "page is owned by " + page.owner + ", never deleted by this script")
                    continue
                }
                List<String> refs = remainingReferences(ctx, page)
                if (!refs.isEmpty()) {
                    outcome(page.path, "SKIPPED", "SKIP_PAGE_STILL_REFERENCED", "still used by " + refs)
                    continue
                }
                if (!session.nodeExists(page.path)) {
                    outcome(page.path, "SKIPPED", "SKIP_PAGE_GONE", "page node no longer exists")
                    continue
                }
                Node pageNode = session.getNode(page.path)
                info("deleting orphaned landing page " + page.path + " (backup of its structure follows):\n" + dump(pageNode, 8))
                page.containers.findAll { it.referencePath != null }.each {
                    warn("  page used container reference " + it.relPath + " -> " + it.referencePath + "; the referenced container is NOT deleted (it may be shared)")
                }
                pageNode.remove()
                session.save()
                changed = true
                outcome(page.path, "UPDATED", "PAGE_DELETED", "no sitemap item references it anymore")
            } catch (Exception e) {
                error("failed to delete page " + page.path, e)
                safeRefresh()
                outcome(page.path, "FAILED", "FAIL_REPOSITORY", e.getMessage())
            }
        }
        return changed
    }

    /** sitemap items (own config + configurations inheriting this workspace) that still point at the page */
    List<String> remainingReferences(Ctx ctx, Page page) {
        List<String> refs = []
        List<Node> configs = [ctx.configNode] + ctx.workspaceInheritors
        for (Node cfg : configs) {
            for (String rel : ["hst:sitemap", "hst:workspace/hst:sitemap"]) {
                if (cfg.hasNode(rel)) collectReferences(cfg.getNode(rel), page.id, refs)
            }
        }
        return refs
    }

    void collectReferences(Node node, String ccid, List<String> refs) {
        for (NodeIterator it = node.getNodes(); it.hasNext();) {
            Node child = it.nextNode()
            if (stringProp(child, "hst:componentconfigurationid") == ccid || multiValue(child, "hst:componentconfigurationmappingvalues").contains(ccid)) {
                refs << child.getPath()
            }
            collectReferences(child, ccid, refs)
        }
    }

    // ============================================================================================================
    // ===== CUSTOMIZATION HOOKS =====
    // Client forks only change the methods in this block. The generic converter keeps them empty.
    // ============================================================================================================

    /** write document fields on the (unpublished) XPage document variant, called before save/publish */
    void populateDocumentVariant(Node variant, Item item, Ctx ctx) {
    }

    /** return false to leave a container item out of the XPage document (e.g. when a document field replaces it) */
    boolean keepContainerItem(Node containerItem, Item item) {
        return true
    }

    // ===== END CUSTOMIZATION HOOKS =====

    // ============================================================================================================
    // repository helpers
    // ============================================================================================================

    Workflow workflow(String category, Node node) {
        return ((HippoWorkspace) session.getWorkspace()).getWorkflowManager().getWorkflow(category, node)
    }

    Node contentNode(Ctx ctx, String relPath) {
        if (ctx.contentRoot == null || StringUtils.isBlank(relPath)) return null
        return safeHasNode(ctx.contentRoot, relPath) ? ctx.contentRoot.getNode(relPath) : null
    }

    boolean safeHasNode(Node node, String relPath) {
        try {
            return node.hasNode(relPath)
        } catch (RepositoryException e) {
            trace("invalid relative path '" + relPath + "' below " + node.getPath() + ": " + e.getMessage())
            return false
        }
    }

    boolean isXPageHandle(Node node) {
        if (node == null || !node.isNodeType("hippo:handle")) return false
        Node unpublished = variants(node).get("unpublished")
        return unpublished != null && unpublished.isNodeType("hst:xpagemixin") && unpublished.hasNode("hst:xpage")
    }

    Map<String, Node> variants(Node handle) {
        Map<String, Node> result = [:]
        for (NodeIterator it = handle.getNodes(handle.getName()); it.hasNext();) {
            Node variant = it.nextNode()
            String state = stringProp(variant, "hippostd:state")
            if (state != null) result.put(state, variant)
        }
        return result
    }

    boolean isLive(Node variant) {
        return multiValue(variant, "hippo:availability").contains("live")
    }

    boolean isBeingEdited(Node handle) {
        Node draft = variants(handle).get("draft")
        return draft != null && StringUtils.isNotBlank(stringProp(draft, "hippostd:holder"))
    }

    String editor(Node handle) {
        return stringProp(variants(handle).get("draft"), "hippostd:holder")
    }

    boolean hasPendingChanges(Map<String, Node> variants) {
        Node unpublished = variants.get("unpublished")
        Node published = variants.get("published")
        if (unpublished == null || published == null || !isLive(published)) return false
        try {
            Calendar modified = unpublished.getProperty("hippostdpubwf:lastModificationDate").getDate()
            Calendar publishedAt = published.hasProperty("hippostdpubwf:publicationDate") ? published.getProperty("hippostdpubwf:publicationDate").getDate()
                    : published.getProperty("hippostdpubwf:lastModificationDate").getDate()
            return modified.after(publishedAt)
        } catch (RepositoryException ignored) {
            return false
        }
    }

    /** deep copy without jcr:* system properties and lock/modification bookkeeping */
    Node copyTree(Node source, Node targetParent, String name, String primaryType, boolean asLayout) {
        Node copy = targetParent.addNode(name, primaryType != null ? primaryType : source.getPrimaryNodeType().getName())
        source.getMixinNodeTypes().each { mixin ->
            String mixinName = mixin.getName()
            if (mixinName != "hst:prototypemeta" && mixinName != "mix:referenceable" && copy.canAddMixin(mixinName)) copy.addMixin(mixinName)
        }
        copyProperties(source, copy, asLayout)
        for (NodeIterator it = source.getNodes(); it.hasNext();) {
            Node child = it.nextNode()
            copyTree(child, copy, child.getName(), null, asLayout)
        }
        return copy
    }

    void copyProperties(Node source, Node target, boolean asLayout) {
        for (PropertyIterator it = source.getProperties(); it.hasNext();) {
            Property property = it.nextProperty()
            String name = property.getName()
            if (name.startsWith("jcr:") || NON_COPY_PROPERTIES.contains(name)) continue
            if (asLayout && PROTOTYPE_ONLY_PROPERTIES.contains(name)) continue
            // layouts keep the prototype's container identifiers on purpose: pages created from that prototype carry the same
            // identifiers, which makes the landing page -> layout container mapping exact. Document containers get new ones.
            if (!asLayout && name == "hippo:identifier" && target.isNodeType("hst:containercomponent")) continue
            if (property.getDefinition().isProtected()) continue
            try {
                if (property.isMultiple()) {
                    target.setProperty(name, property.getValues())
                } else {
                    target.setProperty(name, property.getValue())
                }
            } catch (RepositoryException e) {
                warn("could not copy property " + property.getPath() + " to " + target.getPath() + ": " + e.getMessage())
            }
        }
    }

    String uniqueChildName(Node parent, String name) {
        if (!parent.hasNode(name)) return name
        int i = 1
        while (parent.hasNode(name + "-" + i)) i++
        return name + "-" + i
    }

    boolean hasComponentClass(Node node) {
        return node.hasProperty("hst:componentclassname") || node.hasProperty("hst:componentdefinition")
    }

    String componentClass(Node node) {
        return stringProp(node, "hst:componentclassname") ?: (stringProp(node, "hst:componentdefinition") ? "def:" + stringProp(node, "hst:componentdefinition") : "-")
    }

    boolean hasDocumentParameter(Node node) {
        return multiValue(node, "hst:parameternames").any { it == "document" || it.toLowerCase().endsWith("document") }
    }

    static String normalizeXType(String xtype) {
        return xtype == null ? null : xtype.replace(':', '.').toLowerCase()
    }

    static String leafName(String path) {
        return path.contains("/") ? path.substring(path.lastIndexOf('/') + 1) : path
    }

    static String stringProp(Node node, String name) {
        if (node == null) return null
        try {
            if (!node.hasProperty(name)) return null
            Property p = node.getProperty(name)
            return p.isMultiple() ? (p.getValues().length > 0 ? p.getValues()[0].getString() : null) : p.getString()
        } catch (RepositoryException ignored) {
            return null
        }
    }

    static List<String> multiValue(Node node, String name) {
        if (node == null || !node.hasProperty(name)) return []
        Property p = node.getProperty(name)
        if (!p.isMultiple()) return [p.getString()]
        return p.getValues().collect { Value v -> v.getString() }
    }

    void safeRefresh() {
        try {
            session.refresh(false)
        } catch (RepositoryException e) {
            log.error("[P2X] session refresh failed", e)
        }
    }

    /** readable dump of a node tree, used in trace output and as a backup before anything is deleted */
    String dump(Node node, int maxDepth) {
        StringBuilder sb = new StringBuilder()
        dumpInto(node, 0, maxDepth, sb)
        return sb.toString()
    }

    void dumpInto(Node node, int depth, int maxDepth, StringBuilder sb) {
        String indent = "    " + "  " * depth
        sb.append(indent).append(depth == 0 ? node.getPath() : node.getName()).append(" [").append(node.getPrimaryNodeType().getName()).append("]\n")
        for (PropertyIterator it = node.getProperties(); it.hasNext();) {
            Property p = it.nextProperty()
            if (p.getName() == "jcr:primaryType" || p.getName() == "jcr:uuid") continue
            String value
            try {
                value = p.isMultiple() ? p.getValues().collect { it.getString() }.toString() : (p.getType() == PropertyType.BINARY ? "<binary>" : p.getString())
            } catch (RepositoryException e) {
                value = "<" + e.getMessage() + ">"
            }
            sb.append(indent).append("  - ").append(p.getName()).append(" = ").append(StringUtils.abbreviate(value, 300)).append("\n")
        }
        if (depth < maxDepth) {
            for (NodeIterator it = node.getNodes(); it.hasNext();) dumpInto(it.nextNode(), depth + 1, maxDepth, sb)
        }
    }

    // ============================================================================================================
    // logging & reporting
    // ============================================================================================================

    String prefix() {
        StringBuilder sb = new StringBuilder("[P2X][").append(logConfig).append("][").append(logStep).append("]")
        if (logTotal > 0) sb.append("[").append(logIndex).append("/").append(logTotal).append("]")
        if (logItem != null) sb.append("[").append(logItem).append("]")
        return sb.append(" ").toString()
    }

    void info(String message) { log.info(prefix() + message) }

    void itemLog(String message) { if (logLevel >= LOG_ITEM) log.info(prefix() + message) }

    void trace(String message) { if (logLevel >= LOG_TRACE) log.info(prefix() + message) }

    void warn(String message) { log.warn(prefix() + message) }

    void error(String message, Throwable t) {
        log.error(prefix() + message + " :: " + t.getClass().getName() + ": " + t.getMessage(), t)
    }

    void outcome(String path, String status, String reason, String detail, Map<String, Object> extra = null) {
        String key = logStep + ":" + status + ":" + reason
        outcomeCounts.put(key, (outcomeCounts.get(key) ?: 0) + 1)
        Map<String, Object> record = new LinkedHashMap<>()
        record.put("step", logStep)
        record.put("item", logItem)
        record.put("path", path)
        record.put("status", status)
        record.put("reason", reason)
        record.put("detail", detail)
        if (extra != null) record.putAll(extra)
        outcomeRecords << record
        String line = "OUTCOME status=" + status + " reason=" + reason + (detail ? " detail=" + detail : "")
        if (status == "FAILED" || status == "ABORTED") {
            log.error(prefix() + line)
        } else if (status == "SKIPPED" && !(reason in ["SKIP_LAYOUT_EXISTS", "SKIP_EXISTING_DOCUMENT", "SKIP_NOT_CONVERTED", "ALREADY_CONVERTED"])) {
            log.warn(prefix() + line)
        } else {
            itemLog(line)
        }
        if (visitorContext != null && path != null) {
            if (status == "UPDATED") visitorContext.reportUpdated(path)
            else if (status == "FAILED" || status == "ABORTED") visitorContext.reportFailed(path)
            else if (MUTATING_STEPS.contains(logStep)) visitorContext.reportSkipped(path)
        }
    }

    Map<String, Object> planRecord(Item item) {
        Map<String, Object> r = new LinkedHashMap<>()
        r.put("page", item.page?.path)
        r.put("pageOwner", item.page?.owner)
        r.put("layout", item.match?.layout?.name)
        r.put("layoutHow", item.match?.how)
        r.put("containers", item.match?.mapping?.collect { k, v -> k.relPath + "->" + v.relPath })
        r.put("originalRelativeContentPath", item.relativeContentPath)
        r.put("target", item.targetRelPath)
        r.put("targetState", item.targetState)
        if (item.page != null && item.relativeContentPath != null && !item.page.contentBeanComponents.isEmpty()) {
            r.put("contentBeanComponents", item.page.contentBeanComponents)
        }
        return r
    }

    void progress(int done, int total, long start) {
        if (progressEvery <= 0 || (done % progressEvery != 0 && done != total)) return
        long elapsed = System.currentTimeMillis() - start
        long eta = done > 0 ? (long) (elapsed / done * (total - done)) : 0
        String saved = logItem
        logItem = null
        info(String.format("progress %d/%d, elapsed %ds, eta %ds", done, total, (long) (elapsed / 1000), (long) (eta / 1000)))
        logItem = saved
    }

    void writeReport(Node configNode) {
        Map<String, Integer> counts = new TreeMap<>()
        outcomeRecords.each { r -> String k = r.get("status") + ":" + r.get("reason"); counts.put(k, (counts.get(k) ?: 0) + 1) }
        info("summary: " + (counts.isEmpty() ? "nothing to report" : counts.collect { k, v -> k + "=" + v }.join(", ")))
        Map<String, Object> report = new LinkedHashMap<>()
        report.put("version", VERSION)
        report.put("configuration", configNode.getPath())
        report.put("step", logStep)
        report.put("dryRun", dryRun)
        report.put("timestamp", new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format(new Date()))
        report.put("counts", counts)
        report.put("items", outcomeRecords)
        String json = toJson(report)
        if (logLevel >= LOG_ITEM) {
            log.info(prefix() + "P2X-REPORT-BEGIN\n" + json + "\nP2X-REPORT-END")
        }
        if (reportToRepository && !dryRun) {
            try {
                Node root = session.getRootNode()
                Node reports = root.hasNode("p2x-reports") ? root.getNode("p2x-reports") : root.addNode("p2x-reports", "nt:unstructured")
                Node forConfig = reports.hasNode(configNode.getName()) ? reports.getNode(configNode.getName()) : reports.addNode(configNode.getName(), "nt:unstructured")
                Node r = forConfig.addNode(logStep + "-" + new java.text.SimpleDateFormat("yyyyMMdd-HHmmss-SSS").format(new Date()), "nt:unstructured")
                r.setProperty("report", json)
                session.save()
                info("report stored at " + r.getPath())
            } catch (RepositoryException e) {
                error("could not store report in repository", e)
                safeRefresh()
            }
        }
    }

    static String toJson(Object value) {
        if (value == null) return "null"
        if (value instanceof Map) {
            return "{" + ((Map) value).collect { k, v -> toJson(String.valueOf(k)) + ":" + toJson(v) }.join(",") + "}"
        }
        if (value instanceof Collection) {
            return "[" + ((Collection) value).collect { toJson(it) }.join(",") + "]"
        }
        if (value instanceof Number || value instanceof Boolean) return String.valueOf(value)
        String s = String.valueOf(value)
        StringBuilder sb = new StringBuilder("\"")
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"': sb.append("\\\""); break
                case '\\': sb.append("\\\\"); break
                case '\n': sb.append("\\n"); break
                case '\r': sb.append("\\r"); break
                case '\t': sb.append("\\t"); break
                default:
                    if (c < 0x20) sb.append(String.format("\\u%04x", (int) c)) else sb.append(c)
            }
        }
        return sb.append("\"").toString()
    }

    // ============================================================================================================
    // parameter parsing (values arrive from JSON: strings, booleans, numbers, lists or maps)
    // ============================================================================================================

    Map<String, Object> effectiveParameters() {
        Map<String, Object> m = new LinkedHashMap<>()
        m.put("hstConfigurations", hstConfigurations)
        m.put("steps", steps)
        m.put("dryRun (updater checkbox)", dryRun)
        m.put("defaultXpageDocType", docType)
        m.put("primaryDocumentMode", primaryDocumentMode)
        m.put("xPageFolder", xPageFolder)
        m.put("documentTemplateCategory", documentTemplateCategory)
        m.put("folderTypes", folderTypes)
        m.put("contentRootPath", contentRootPath ?: "(from hst:site)")
        m.put("templateMapping", templateMapping)
        m.put("fallbackXPageTemplate", fallbackXPageTemplate)
        m.put("unmatchedContainers", unmatchedContainers)
        m.put("existingDocument", existingDocument)
        m.put("publish", publish)
        m.put("publishPendingChanges", publishPendingChanges)
        m.put("titleProperty", titleProperty)
        m.put("setDisplayName", setDisplayName)
        m.put("parseInheritedHstConfiguration", parseInheritedHstConfiguration)
        m.put("overwriteXPageTemplates", overwriteXPageTemplates)
        m.put("generateMissingTemplates", generateMissingTemplates)
        m.put("discardPreviewConfiguration", discardPreviewConfiguration)
        m.put("cleanupUnpublished", cleanupUnpublished)
        m.put("includePaths", includePaths)
        m.put("limit", limit)
        m.put("logLevel", ["summary", "item", "trace"][logLevel])
        m.put("progressEvery", progressEvery)
        m.put("reportToRepository", reportToRepository)
        return m
    }

    String strParam(String key, String defaultValue) {
        Object v = parametersMap?.get(key)
        if (v == null) return defaultValue
        String s = String.valueOf(v).trim()
        return s.isEmpty() ? defaultValue : s
    }

    boolean boolParam(String key, boolean defaultValue) {
        Object v = parametersMap?.get(key)
        if (v == null) return defaultValue
        if (v instanceof Boolean) return (Boolean) v
        String s = String.valueOf(v).trim().toLowerCase()
        if (s in ["true", "yes", "1"]) return true
        if (s in ["false", "no", "0"]) return false
        throw new IllegalArgumentException("[P2X] parameter " + key + " must be true or false, got '" + v + "'")
    }

    int intParam(String key, int defaultValue) {
        Object v = parametersMap?.get(key)
        if (v == null) return defaultValue
        if (v instanceof Number) return ((Number) v).intValue()
        return Integer.parseInt(String.valueOf(v).trim())
    }

    List<String> listParam(String key, List<String> defaultValue) {
        Object v = parametersMap?.get(key)
        if (v == null) return defaultValue == null ? [] : defaultValue
        List<String> result
        if (v instanceof Collection) {
            result = ((Collection) v).collect { String.valueOf(it).trim() }
        } else {
            result = String.valueOf(v).split(",").collect { it.trim() }
        }
        result = result.findAll { !it.isEmpty() }
        return result.isEmpty() && defaultValue != null ? defaultValue : result
    }

    Map<String, String> mapParam(String key) {
        Object v = parametersMap?.get(key)
        Map<String, String> result = new LinkedHashMap<>()
        if (v instanceof Map) {
            ((Map) v).each { k, val -> result.put(StringUtils.strip(String.valueOf(k), "/"), String.valueOf(val)) }
        } else if (v != null) {
            throw new IllegalArgumentException("[P2X] parameter " + key + " must be a JSON object, e.g. {\"about\": \"one-column\"}")
        }
        return result
    }

    static int parseLogLevel(String level) {
        switch (level.toLowerCase()) {
            case "summary": return LOG_SUMMARY
            case "trace": return LOG_TRACE
            default: return LOG_ITEM
        }
    }
}
