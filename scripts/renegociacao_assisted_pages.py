"""Publish only the two assisted-demo pages; preserve the self-service page."""
import argparse
import re
import time
from pathlib import Path

from azure.identity import AzureCliCredential
from PowerPlatform.Dataverse.client import DataverseClient

from renegociacao_pages import PageDeployment, MARKER, ORG, ROOT, SITE
from renegociacao_schema import Deployment
from auth import _build_operation_context

PAGES = [
    ("portal-assistido", "Portal Assistido", "assisted"),
    ("aceite-renegociacao", "Aceite da renegociacao", "acceptance"),
]
CHAT_SCRIPT_ORIGINS = (
    "https://oc-cdn-ocprod.azureedge.net",
    "https://ocprodocprodnamgs.blob.core.windows.net",
)


def allow_chat_scripts(policy):
    directives = [part.strip() for part in policy.split(";") if part.strip()]
    names = [part.split()[0].lower() for part in directives]
    if len(names) != len(set(names)) or "script-src" not in names:
        raise RuntimeError("The CSP must have one explicit script-src directive")
    for name in ("script-src", "script-src-elem"):
        if name not in names:
            continue
        index = names.index(name)
        tokens = directives[index].split()
        if "'none'" in tokens:
            raise RuntimeError("An explicit script denial requires a separate security decision")
        directives[index] += "".join(" " + origin for origin in CHAT_SCRIPT_ORIGINS if origin not in tokens)
    return "; ".join(directives) + ";"


def configure_chat_csp(d):
    pages = PageDeployment(d.client, d.backup_dir)
    rows = pages.rows("mspp_sitesetting", "mspp_name eq 'HTTP/Content-Security-Policy'")
    if len(rows) > 1:
        raise RuntimeError("Multiple enforcement CSP settings exist")
    if rows:
        current = rows[0].get("mspp_value") or ""
        if not current.strip():
            raise RuntimeError("CSP is disabled; refusing to replace the security policy implicitly")
        pages.save_before("mspp_sitesetting", rows[0])
    else:
        response = d.session.get(
            "https://portalatendimento01.powerappsportals.com/portal-assistido/",
            allow_redirects=False, timeout=60,
        )
        response.raise_for_status()
        current = response.headers.get("Content-Security-Policy", "")
        if not current:
            raise RuntimeError("The portal's default enforcement CSP could not be read")
        # Power Pages substitutes 'nonce' and generates per-response event hashes.
        current = re.sub(r"'nonce-[^']+'", "'nonce'", current)
        current = re.sub(r"\s+'sha(?:256|384|512)-[^']+'", "", current)
        d.snapshot("default-csp-before.json", {"policy": current, "customSettingExisted": False})
    expected = allow_chat_scripts(current)
    pages.upsert("mspp_sitesetting", "HTTP/Content-Security-Policy", {"mspp_value": expected})
    print("CSP extended only for the two exact Microsoft widget script origins; existing directives preserved.")


def template_source(slug, entry):
    folder = ROOT / "pages" / "renegociacao"
    html = (folder / (entry + ".html")).read_text(encoding="utf-8")
    bundle = (folder / "dist" / (entry + ".js")).read_text(encoding="utf-8")
    tag = f'<script src="/{slug}/{entry}.js" defer></script>'
    if html.count(tag) != 1:
        raise RuntimeError("The assisted bundle placeholder is missing or duplicated")
    bundle = re.sub(r"</script", r"<\\/script", bundle, flags=re.IGNORECASE)
    source = MARKER + "\n" + html.replace(tag, "<script>\n" + bundle + "\n</script>")
    if len(source) > 1_048_576:
        raise RuntimeError("The assisted page exceeds the native web-template source limit")
    return source


def apply(d, publish):
    pages = PageDeployment(d.client, d.backup_dir)
    website = d.client.records.retrieve("mspp_website", SITE)
    if website["mspp_primarydomainname"].lower() != "portalatendimento01.powerappsportals.com":
        raise RuntimeError("The existing portal target changed")
    home = pages.one("mspp_webpage", "mspp_partialurl eq '/' and mspp_isroot eq true")
    locales = pages.rows("mspp_webpage", f"_mspp_rootwebpageid_value eq {home['mspp_webpageid']} and mspp_isroot eq false")
    if len(locales) != 1 or not locales[0].get("_mspp_webpagelanguageid_value"):
        raise RuntimeError("The portal language is ambiguous")
    language = locales[0]["_mspp_webpagelanguageid_value"]
    templates = pages.rows("mspp_pagetemplate", "_mspp_webtemplateid_value ne null")
    if not templates:
        raise RuntimeError("No native page-template type exists")
    state = pages.one("mspp_publishingstate", "mspp_name eq '" + ("Published" if publish else "Draft") + "'")
    for slug, title, entry in PAGES:
        name = "FSI - " + title
        conflicts = pages.rows("mspp_webpage", f"mspp_partialurl eq '{slug}'")
        if any(r["mspp_name"] not in (name + " (root)", name + " (pt-BR)") for r in conflicts):
            raise RuntimeError("An unrelated page already owns this assisted URL")
        web = pages.upsert("mspp_webtemplate", name, {
            "mspp_source": template_source(slug, entry), "mspp_mimetype": "text/html",
        })
        template = pages.upsert("mspp_pagetemplate", name, {
            "mspp_type": templates[0]["mspp_type"], "mspp_usewebsiteheaderandfooter": True,
            "mspp_webtemplateid@odata.bind": f"/mspp_webtemplates({web})",
        })
        common = {
            "mspp_title": title, "mspp_partialurl": slug, "mspp_hiddenfromsitemap": True,
            "mspp_excludefromsearch": True,
            "mspp_pagetemplateid@odata.bind": f"/mspp_pagetemplates({template})",
            "mspp_parentpageid@odata.bind": f"/mspp_webpages({home['mspp_webpageid']})",
            "mspp_publishingstateid@odata.bind": f"/mspp_publishingstates({state['mspp_publishingstateid']})",
        }
        root = pages.upsert("mspp_webpage", name + " (root)", {**common, "mspp_isroot": True})
        for attempt in range(10):
            localized = pages.rows("mspp_webpage", f"_mspp_rootwebpageid_value eq {root} and mspp_isroot eq false")
            if localized:
                break
            time.sleep(2)
        if len(localized) > 1:
            raise RuntimeError("Duplicate localizations found; no records were deleted")
        pages.upsert("mspp_webpage", name + " (pt-BR)", {
            **common, "mspp_isroot": False, "mspp_rootwebpageid@odata.bind": f"/mspp_webpages({root})",
            "mspp_webpagelanguageid@odata.bind": f"/mspp_websitelanguages({language})",
        })
        print("ASSISTED_PAGE", "Published" if publish else "Draft",
              "https://portalatendimento01.powerappsportals.com/" + slug + "/")
    print("Existing self-service page, site navigation, bot and chat workstream were not changed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--backup-dir", type=Path, required=True)
    parser.add_argument("--publish", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--enable-omnichannel-csp", action="store_true",
                        help="Requires explicit approval to add only the provided widget's Microsoft script origins")
    args = parser.parse_args()
    if args.environment.rstrip("/") != ORG or not args.apply:
        parser.error("Use --apply only for the approved CCaaS environment")
    import dnsfix
    dnsfix.install()
    deployment = Deployment(args.backup_dir)
    try:
        deployment.credential = AzureCliCredential(process_timeout=90)
        deployment.client.close()
        deployment.client = DataverseClient(
            base_url=ORG, credential=deployment.credential, context=_build_operation_context("dv-metadata"),
        )
        deployment.preflight()
        if args.enable_omnichannel_csp:
            configure_chat_csp(deployment)
        else:
            apply(deployment, args.publish)
    finally:
        deployment.close()
