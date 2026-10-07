# Landing page → Experience Page (XPage) conversion

`landing2xpageconversion.groovy` is a brXM **17.x** repository updater. It turns the landing pages that editors built in a
channel (workspace `hst:pages` + `hst:sitemap` items) into Experience Page documents, keeping the page layout and the
container items as they are, so editors keep the experience they are used to.

| File | Purpose |
|---|---|
| `landing2xpageconversion.groovy` | the script |
| `landing2xpageconversion.yaml` | the same script as an updater registry entry |

The script is a starting point, real projects differ. You should copy it into your project and adapt it from there.
The demo project in `../../demo` (brXM 17.3.0) covers the most common case: it has the updater registered, a fieldless
`myproject:xpagedocument` document type, and test fixtures under the `/p2x/**` sitemap items.

---

## Running the updater

*Setup → System → Updater editor*, select **XPath query** and query **only** the configurations node of the runtime
HST root:

```
/jcr:root/hst:myproject/hst:configurations
```

Set log level to `INFO` (the script has its own levels).

### Parameters

| Parameter | Default | Meaning |
|---|---|---|
| `hstConfigurations` | – (required) | comma separated configuration names (`hstConfiguration` also accepted) |
| `steps` | `analyze` | `analyze`, `templates`, `convert`, `sitemap`, `cleanup` (comma separated) |
| `defaultXpageDocType` | – (required for convert) | e.g. `myproject:xpagedocument` |
| `primaryDocumentMode` | `connector` | `connector` or `mixin`, see above |
| `xPageFolder` | `xpages` | folder below the content root that receives the documents |
| `documentTemplateCategory` | `new-xpage-document` | template query used by the folder workflow to add documents |
| `folderTypes` | `["new-xpage-document","new-xpage-folder"]` | added to `hippostd:foldertype` of the XPage folders |
| `contentRootPath` | from `hst:site` | content root override when the site cannot be resolved |
| `templateMapping` | `{}` | `{"<sitemap path or page name>": "<layout>"}` explicit overrides |
| `fallbackXPageTemplate` | – | layout used when no layout matches |
| `unmatchedContainers` | `skip-page` | `skip-page` or `merge-into-primary` |
| `existingDocument` | `skip` | `skip`, `reapply` (re-copy the layout into the existing XPage) or `recreate` (delete + create) |
| `publish` | `true` | publish converted documents (`publishPages` also accepted) |
| `publishPendingChanges` | `false` | also publish existing documents that had unpublished editor changes |
| `titleProperty` | – | document property to fill with the sitemap item's `hst:pagetitle` (e.g. `acm:title`) |
| `setDisplayName` | `true` | use `hst:pagetitle` as the document display name |
| `parseInheritedHstConfiguration` | `false` | also convert prototype pages of inherited configurations |
| `overwriteXPageTemplates` | `false` | let `templates` replace existing layouts |
| `generateMissingTemplates` | `false` | let `templates` derive layouts from pages that nothing else can host |
| `discardPreviewConfiguration` | `true` | delete `<config>-preview` before writing; `false` aborts instead |
| `cleanupUnpublished` | `false` | let `cleanup` repoint/remove items whose XPage is not published yet |
| `sitemapStrategy` | `wildcard` | `wildcard` (remove converted items, URLs via the `_default_` chain) or `explicit` (keep and repoint them) |
| `includePaths` | – | only handle these sitemap paths (and below) |
| `limit` | `0` | convert at most N items per run |
| `logLevel` | `item` | `summary`, `item` or `trace` |
| `progressEvery` | `25` | progress / ETA line every N items |
| `reportToRepository` | `false` | also store each step report under `/p2x-reports` |

---

## Why steps

HST structures differ a lot in real life projects: inherited configurations, container references, pages shared by many sitemap
items, absolute content paths, name collisions in the content tree, preview configurations with unpublished changes...
Running everything in one go can result in a half converted channel. The conversion is therefore split into
separate steps that you run one at a time and verify in between.

| Step | Writes | What it does |
|---|---|---|
| `analyze` *(default)* | nothing | Builds the model and logs, per sitemap item: the resolved page, the chosen layout and how every container maps, the target document and whether it collides, plus warnings (components that rely on the content bean, missing documents, preview configuration, missing doc type / template query). |
| `templates` | HST config | Copies `hst:prototypepages` into `hst:xpages` layouts. Existing layouts are never overwritten unless `overwriteXPageTemplates=true`. With `generateMissingTemplates=true` it also creates a layout for every page structure that no layout can host. |
| `convert` | content | Creates one XPage document per convertible sitemap item under `<content root>/<xPageFolder>/<sitemap path>` and copies the landing page's container items into it. **Deletes nothing.** Existing XPage documents are skipped (`existingDocument`). |
| `sitemap` | HST config | `sitemapStrategy: wildcard` *(default)*: adds a root `_default_` chain to `hst:sitemap` that maps URL slugs onto the XPage folder (see below). Existing wildcards only get a `hst:relativecontentpath` added; a wildcard that already maps elsewhere aborts the step. Does nothing with `explicit`. |
| `cleanup` | HST config | For each sitemap item whose XPage document is published: **removes** the item when the wildcard chain serves its URL with the same document, otherwise removes `hst:componentconfigurationid` and points `hst:relativecontentpath` at the document. Either way the URL stays exactly the same. Then deletes the workspace pages that no sitemap item uses anymore (a full dump of each deleted item and page is logged first). Inherited / non-workspace pages are never deleted. |

Recommended sequence:

1. `analyze` – read the report, fix what it flags (template mapping, doc type, preview configuration...).
2. `templates` – run locally; export the resulting `hst:xpages` to the project's HCM config and deploy it.
3. `analyze` - again. every item should now say `CONVERT` or have a skip reason you accept.
4. `convert` – then check the new documents in the Experience manager. The site still serves the old pages.
5. `sitemap` – adds the wildcard chain (the explicit landing page items still win, so nothing changes yet).
6. `cleanup` – switches the URLs to the XPage documents.

Every step can be re-run: already converted items are reported as `ALREADY_CONVERTED` / `SKIP_EXISTING_DOCUMENT`.
Several steps can be combined (`"steps": "convert,sitemap,cleanup"`); they always execute in the order above.

Use `includePaths` and `limit` to convert a large channel in batches.

### Mutating steps refuse to run when...

* a `<config>-preview` configuration exists and `discardPreviewConfiguration=false`. By default the preview
  configuration is deleted (with a warning and a dump in the log) before the step runs, so unpublished channel changes
  are lost. Publish or discard them in the Experience manager first, or set `discardPreviewConfiguration=false` to
  make the script stop instead.
* the updater's **Dry run** checkbox is ticked: a dry run only ever executes `analyze`.

### Sitemap strategy

`sitemapStrategy: "explicit"` – every converted item stays, pointing at its document.
`sitemapStrategy: "wildcard"` *(default)* – the URL of an XPage is its path in the XPage folder:

```
hst:sitemap
  _default_          hst:relativecontentpath = xpages/${1}        (+ hst:componentconfigurationid of the root _any_, e.g. 404)
    _index_          hst:relativecontentpath = ${parent}/index
    _default_        hst:relativecontentpath = ${parent}/${2}
      _index_        ...                                           one level per URL depth that was converted
```

`/foo/bar` resolves to `xpages/foo/bar`, or to `xpages/foo/bar/index` when `bar` has child pages. When the document does
not exist or is unpublished, the wildcard's `hst:componentconfigurationid` is used as fallback, so the 404 page renders.
Converted sitemap items are therefore removed: deleting or unpublishing an XPage later needs no sitemap clean up, and new
XPages that editors create in the folder get a URL automatically. Explicit sitemap items always win over wildcards, so
pages that were not converted keep working.

A converted item is kept (and repointed instead) when:
* it has properties of its own besides the page reference and page title (`hst:refId`, `hst:roles`, `hst:scheme`,
  `hst:namedpipeline`, `hst:parameternames`, ...), or child items that stay;
* its document is outside the XPage folder (`primaryDocumentMode: "mixin"`);
* an explicit ancestor has its own wildcard children (e.g. `/news/_any_`) that would match the URL first.

`analyze` shows per item which of the two will happen (`sitemap :` line).

### Resetting a test environment

To reset after a script execution in the demo run:
`mvn clean verify` (removes `target/storage`) followed by `mvn -P cargo.run`. 
On a real environment: restore the database backup taken before the run.

---

## Target document type: the connector document

By default, every page is converted into **one fieldless document type** (`<ns>:xpagedocument`). It carries no content;
it only exists to connect the page to the document workflow that Experience Pages bring (drafts, publication,
scheduling, versions, requests). The layout and components stay identical, so for editors the page looks and behaves
like before. Converting pages into "real" content types with their own templates is possible, but in practice it
confuses editors and changes the site.

A real project needs such a document type (or whichever type the pages should become) plus a template query for it.
The demo's `myproject:xpagedocument`, `new-xpage-document` and `new-xpage-folder` are an example. Point
`defaultXpageDocType`, `documentTemplateCategory` and `folderTypes` at your own names.

### Pages powered by a document (`hst:relativecontentpath`)

* `primaryDocumentMode: "connector"` *(default)*: a connector document is created, the original content document is left
  untouched. **Caveat:** after conversion the content bean of the page is the connector document. Components that read
  `getContentBean()` (no explicit `document` parameter, e.g. `EssentialsContentComponent`) will render empty. `analyze`
  lists these components per page (`CONTENT BEAN` warning).
* `primaryDocumentMode: "mixin"`: the XPage is added to the existing content document itself (its type must allow
  `hst:xpagemixin`). Use this for the pages flagged above – a mixed run is fine:
  `{"steps": "convert,sitemap,cleanup", "primaryDocumentMode": "mixin", "includePaths": "products/details"}` first,
  then the normal run. Run the mixin batch including `cleanup`, otherwise the normal run converts those items again.

### Customization hooks

The script has a marked `CUSTOMIZATION HOOKS` block. Project-specific forks need only to change those methods. 
Upstream fixes can be merged with a plain diff:

* `populateDocumentVariant(variant, item, ctx)` – write document fields before save/publish
* `keepContainerItem(containerItem, item)` – return `false` to leave an item out (e.g. because a field replaces it)

---

## How pages are matched to layouts

For each landing (prototype) page, every layout is scored by mapping each landing container to a layout container by
**`hippo:identifier`**, then **relative path** (`main/content/left`), then a **unique container name**. A layout wins when
all containers map without two landing containers landing in the same layout container; ties are broken by equal
static structure, then equal `hst:referencecomponent`, then name (deterministic).

Nothing is ever dropped silently: a page with unmatched or colliding containers is skipped with
`SKIP_UNMATCHED_CONTAINER` unless you choose `templateMapping`, `fallbackXPageTemplate`,
`unmatchedContainers: "merge-into-primary"` or `generateMissingTemplates`.

Container references (`hst:containercomponentreference`) are resolved to `hst:workspace/hst:containers`. Pages are
resolved in the workspace, the configuration and the `hst:inheritsfrom` configurations.

---

## Logging

The updater run cannot be paused or debugged, so the log is designed to explain every decision:

* every line is prefixed `[P2X][<configuration>][<step>][<n>/<total>][<sitemap path>]` – grep one item across steps;
* every item ends with `OUTCOME status=<UPDATED|SKIPPED|FAILED|...> reason=<CODE> detail=...` using stable reason codes
  (`SKIP_NO_PAGE`, `SKIP_NO_TEMPLATE`, `SKIP_UNMATCHED_CONTAINER`, `SKIP_TARGET_CONFLICT`, `SKIP_EXISTING_DOCUMENT`,
  `ALREADY_CONVERTED`, `CONVERTED`, `SITEMAP_ITEM_REPOINTED`, `PAGE_DELETED`, `FAIL_WORKFLOW`, `FAIL_REPOSITORY`, ...);
* `analyze` prints a fixed-width table (sitemap item | page | layout | target | result);
* every step ends with a summary and a JSON report between `P2X-REPORT-BEGIN` / `P2X-REPORT-END` that can be diffed between runs;
* everything deleted is dumped to the log first; `logLevel: "trace"` adds layout scoring and full node dumps;
* an error on one item is logged with its context, the session is refreshed and the next item continues.

---

## Development

When the groovy script changes, update the `hipposys:script` in `landing2xpageconversion.yaml` and in the demo's
updater registry entry as well. 

Start the demo with auto export disabled so test runs do not end up in the demo's YAML:
```
cd demo && mvn clean verify && mvn -P cargo.run -Drepo.autoexport.allowed=false
```
