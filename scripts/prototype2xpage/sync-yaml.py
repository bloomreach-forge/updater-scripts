#!/usr/bin/env python3
"""
Embed the canonical groovy scripts into updater-registry YAML so the copies can never drift.

  scripts/prototype2xpage/landing2xpageconversion.groovy  (source of truth)
    -> scripts/prototype2xpage/landing2xpageconversion.yaml                      (drop-in for customer projects)
    -> demo/.../hcm-config/configuration/update/registry/landing2xpageconversion.yaml

Usage:  python3 scripts/prototype2xpage/sync-yaml.py [--check]
  --check  exit 1 when a generated file is out of date (for CI / pre-commit)
"""
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
REGISTRY = REPO / "demo/repository-data/application/src/main/resources/hcm-config/configuration/update/registry"

CONVERTER_DESCRIPTION = (
    "Landing page -> Experience Page converter. Run step by step: analyze, templates, sitemap, convert, cleanup. "
    "See scripts/prototype2xpage/README.md")

DEMO_PARAMETERS = {
    "hstConfigurations": "myproject",
    "steps": "analyze",
    "defaultXpageDocType": "myproject:xpagedocument",
    "xPageFolder": "xpages",
    "documentTemplateCategory": "new-xpage-document",
    "folderTypes": ["new-xpage-document", "new-xpage-folder"],
    "publish": True,
    "logLevel": "item",
}

TEMPLATE_PARAMETERS = {
    "hstConfigurations": "<your channel configuration, e.g. myproject>",
    "steps": "analyze",
    "defaultXpageDocType": "<namespace>:xpagedocument",
    "xPageFolder": "xpages",
    "documentTemplateCategory": "new-xpage-document",
    "folderTypes": ["new-xpage-document", "new-xpage-folder"],
    "publish": True,
    "logLevel": "item",
}


def yaml_string(value: str) -> str:
    # JSON strings are valid YAML double-quoted scalars
    return json.dumps(value, ensure_ascii=False)


def block(text: str, indent: str) -> str:
    return "\n".join((indent + line) if line.strip() else "" for line in text.rstrip("\n").split("\n"))


def updater_yaml(node_name: str, script: str, parameters: dict, query: str, description: str) -> str:
    # An XPath query that matches only the hst:configurations node: a repository *path* would make the updater visit
    # (and throttle on) every descendant HST node. Use the *runtime* HST root: HCM YAML is written as /hst:hst/... and
    # remapped by the site's hstRoot (hcm-site.yaml), e.g. /hst:myproject/hst:configurations for the demo.
    return (
        "definitions:\n"
        "  config:\n"
        f"    /hippo:configuration/hippo:update/hippo:registry/{node_name}:\n"
        "      jcr:primaryType: hipposys:updaterinfo\n"
        "      hipposys:batchsize: 10\n"
        f"      hipposys:description: {yaml_string(description)}\n"
        "      hipposys:dryrun: false\n"
        "      hipposys:loglevel: INFO\n"
        f"      hipposys:parameters: {yaml_string(json.dumps(parameters, indent=2))}\n"
        f"      hipposys:query: {query}\n"
        "      hipposys:script: |\n"
        f"{block(script, '        ')}\n"
        "      hipposys:throttle: 1000\n"
    )


def main() -> int:
    check = "--check" in sys.argv
    converter = (HERE / "landing2xpageconversion.groovy").read_text()
    outputs = {
        HERE / "landing2xpageconversion.yaml": updater_yaml(
            "Landing Page to XPage Conversion", converter, TEMPLATE_PARAMETERS,
            "/jcr:root/hst:myproject/hst:configurations", CONVERTER_DESCRIPTION),
        REGISTRY / "landing2xpageconversion.yaml": updater_yaml(
            "Landing Page to XPage Conversion", converter, DEMO_PARAMETERS,
            "/jcr:root/hst:myproject/hst:configurations", CONVERTER_DESCRIPTION),
    }
    stale = []
    for path, content in outputs.items():
        current = path.read_text() if path.exists() else None
        if current == content:
            continue
        stale.append(path)
        if not check:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)
    for path in stale:
        print(("out of date: " if check else "wrote: ") + str(path.relative_to(REPO)))
    if not stale:
        print("all generated updater YAML files are up to date")
    return 1 if (check and stale) else 0


if __name__ == "__main__":
    sys.exit(main())
