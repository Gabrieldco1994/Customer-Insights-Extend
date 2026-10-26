"""Add only the renegotiation page to the existing customer self-service site."""
import argparse
import json
import re
import sys
from pathlib import Path

import requests
from azure.identity import AzureCliCredential
from PowerPlatform.Dataverse.client import DataverseClient

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import dnsfix
from auth import _build_operation_context

ORG = "https://org88957f9a.crm.dynamics.com"
SITE = "15db05fd-f4ce-f111-a05c-000d3a3437e2"
API = "https://fsi-reneg-88957f9a.azurewebsites.net"
SLUG = "renegociacao-financiamento"
MARKER = "<!-- FSI Renegociacao: managed demo component -->"
TEMPLATE = "FSI - Renegociacao de financiamento"
SDK_PATH = ROOT / "pages" / "renegociacao" / "dist" / "chat.js"
SDK_TEMPLATE = "FSI - Renegociacao Chat SDK"
SDK_MARKER = "{% comment %}" + MARKER + "{% endcomment %}"


def sdk_code(source):
    """Keep JS string semantics while excluding characters invalid in Dataverse XML."""
    source = re.sub(r"</script", r"<\\/script", source, flags=re.IGNORECASE)
    return re.sub(
        r"[\x00-\x08\x0b\x0c\x0e-\x1f]",
        lambda match: f"\\u{ord(match.group()):04x}", source,
    )


def sdk_sources(source, size=700_000):
    encoded = json.dumps(sdk_code(source), ensure_ascii=True)
    encoded = encoded.replace("<", "\\u003c").replace("{", "\\u007b").replace("}", "\\u007d")
    return [SDK_MARKER + encoded[index:index + size] for index in range(0, len(encoded), size)]


def expected_fields(payload):
    for key, expected in payload.items():
        if key.endswith("@odata.bind"):
            key = "_" + key.removesuffix("@odata.bind") + "_value"
            expected = expected.split("(")[-1].rstrip(")") if expected else None
        yield key, expected
SETTINGS = {
    "FSI/Renegociacao/ApiUrl": API,
    "FSI/Renegociacao/ClientId": "945076d4-6ec8-4ec7-8cdd-1fe34c686262",
    "FSI/Renegociacao/Audience": "22630d43-3353-4fb7-8496-3979bd7617d9",
    "FSI/Renegociacao/TenantId": "3a3c8be3-6a50-455f-a303-f9df0c768430",
}


def template_source():
    folder = ROOT / "pages" / "renegociacao"
    html = (folder / "page.html").read_text(encoding="utf-8")
    bundle_path = folder / "dist" / "renegociacao.js"
    if not bundle_path.is_file():
        raise RuntimeError("Build the page first: npm --prefix pages/renegociacao run build")
    bundle = re.sub(
        r"</script", r"<\\/script", bundle_path.read_text(encoding="utf-8"),
        flags=re.IGNORECASE,
    )
    tag = '<script src="/renegociacao-financiamento/renegociacao.js" defer></script>'
    if html.count(tag) != 1:
        raise RuntimeError("Expected exactly one page bundle placeholder")
    source = MARKER + "\n" + html.replace(tag, "<script>\n" + bundle + "\n</script>")
    if len(source) > 1_048_576:
        raise RuntimeError("The page template exceeds its native source limit")
    return source


class PageDeployment:
    def __init__(self, client, backup):
        self.client = client
        self.backup = backup
        self.backup.mkdir(parents=True, exist_ok=True)

    def rows(self, table, filter):
        return self.client.records.list(
            table, filter=f"_mspp_websiteid_value eq {SITE} and statecode eq 0 and ({filter})",
            top=4,
        )

    def one(self, table, filter):
        rows = self.rows(table, filter)
        if len(rows) != 1:
            raise RuntimeError(f"Expected one {table}: {filter}; found {len(rows)}")
        return rows[0]

    def save_before(self, table, row):
        primary = self.client.tables.get(table)["primary_id_attribute"]
        target = self.backup / f"{table}-{row[primary]}.json"
        if not target.exists():
            target.write_text(
                json.dumps(dict(row), ensure_ascii=False, indent=2, default=str),
                encoding="utf-8",
            )

    def verify(self, table, identifier, payload):
        row = self.client.records.retrieve(table, identifier)
        for key, expected in expected_fields(payload):
            if row.get(key) != expected:
                raise RuntimeError(f"Read-back mismatch: {table}/{identifier}/{key}")
        return row

    def upsert(self, table, name, payload):
        escaped = name.replace("'", "''")
        filter = f"mspp_name eq '{escaped}'"
        if table == "mspp_webpage":
            filter += f" and mspp_isroot eq {str(payload['mspp_isroot']).lower()}"
        rows = self.rows(table, filter)
        if not rows and table == "mspp_webpage" and not payload["mspp_isroot"]:
            root = payload["mspp_rootwebpageid@odata.bind"].split("(")[-1].rstrip(")")
            language = payload["mspp_webpagelanguageid@odata.bind"].split("(")[-1].rstrip(")")
            # Power Pages creates a localized copy automatically when a root is created.
            rows = [row for row in self.rows(table, (
                f"_mspp_rootwebpageid_value eq {root} and mspp_isroot eq false"
            )) if row.get("_mspp_webpagelanguageid_value") == language]
        payload = {"mspp_name": name, **payload}
        primary = self.client.tables.get(table)["primary_id_attribute"]
        if len(rows) > 1:
            raise RuntimeError(f"Duplicate managed Pages component: {table}/{name}")
        if rows:
            row = rows[0]
            if table == "mspp_webtemplate" and not row.get("mspp_source", "").startswith((MARKER, SDK_MARKER)):
                raise RuntimeError("A different web template already uses the managed name")
            if table == "mspp_webfile" and row.get("mspp_summary") != MARKER:
                raise RuntimeError("A different web file already uses the managed name")
            if all(row.get(key) == expected for key, expected in expected_fields(payload)):
                print("Verified unchanged Pages component:", table, name, row[primary], flush=True)
                return str(row[primary])
            self.save_before(table, row)
            self.client.records.update(table, row[primary], payload)
            identifier = row[primary]
        else:
            identifier = self.client.records.create(table, {
                "mspp_name": name,
                "mspp_websiteid@odata.bind": f"/mspp_websites({SITE})",
                **payload,
            })
        self.verify(table, identifier, payload)
        print("Pages component:", table, name, identifier, flush=True)
        return str(identifier)

    def remove_generated_localized(self, root, language, draft):
        rows = self.client.records.list("mspp_webpage", filter=(
            f"_mspp_websiteid_value eq {SITE} and "
            f"_mspp_rootwebpageid_value eq {root} and mspp_isroot eq false and "
            "mspp_name eq 'FSI - Renegociacao (root)'"
        ), top=4)
        for row in rows:
            if (row.get("_mspp_webpagelanguageid_value") != language or
                    row.get("_mspp_publishingstateid_value") != draft or
                    row.get("mspp_partialurl") != SLUG):
                raise RuntimeError("Unexpected generated localized page; refusing to remove it")
            self.save_before("mspp_webpage", row)
            # Inactive copies still participate in portal localization resolution.
            self.client.records.delete("mspp_webpage", row["mspp_webpageid"])
            remaining = self.client.records.list(
                "mspp_webpage", filter=f"mspp_webpageid eq {row['mspp_webpageid']}", top=1
            )
            if remaining:
                raise RuntimeError("The duplicate Draft localization was not removed")
            print("Removed backed-up duplicate Draft localization:", row["mspp_webpageid"])

    def prepare(self, enable_chat=False):
        source = template_source()
        if enable_chat and not SDK_PATH.is_file():
            raise RuntimeError("Build the optional chat SDK before preparing the page")
        website = self.client.records.retrieve("mspp_website", SITE)
        if website["mspp_primarydomainname"].lower() != "portalatendimento01.powerappsportals.com":
            raise RuntimeError("The existing self-service site no longer matches")
        for page in self.rows("mspp_webpage", f"mspp_partialurl eq '{SLUG}'"):
            if page["mspp_name"] not in (
                "FSI - Renegociacao (root)", "FSI - Renegociacao (pt-BR)"
            ):
                raise RuntimeError("Another page already uses the dedicated renegotiation URL")
        home = self.one("mspp_webpage", "mspp_partialurl eq '/' and mspp_isroot eq true")
        homes = self.rows(
            "mspp_webpage",
            f"_mspp_rootwebpageid_value eq {home['mspp_webpageid']} and mspp_isroot eq false",
        )
        if len(homes) != 1:
            raise RuntimeError("Select the site language explicitly before modifying a multilingual site")
        language = homes[0].get("_mspp_webpagelanguageid_value")
        if not language:
            raise RuntimeError("The home page has no language configuration")
        templates = self.rows("mspp_pagetemplate", "_mspp_webtemplateid_value ne null")
        if not templates:
            raise RuntimeError("No existing web-template page type found on the site")
        page_type = templates[0]["mspp_type"]
        draft = self.one("mspp_publishingstate", "mspp_name eq 'Draft'")
        draft_binding = f"/mspp_publishingstates({draft['mspp_publishingstateid']})"
        settings = {**SETTINGS, "FSI/Renegociacao/ChatEnabled": str(enable_chat).lower()}
        for name, value in settings.items():
            self.upsert("mspp_sitesetting", name, {"mspp_value": value})
        web_template = self.upsert(
            "mspp_webtemplate", TEMPLATE,
            {"mspp_source": source, "mspp_mimetype": "text/html"},
        )
        page_template = self.upsert("mspp_pagetemplate", TEMPLATE, {
            "mspp_type": page_type,
            "mspp_usewebsiteheaderandfooter": True,
            "mspp_webtemplateid@odata.bind": f"/mspp_webtemplates({web_template})",
        })
        common = {
            "mspp_partialurl": SLUG,
            "mspp_title": "Renegociação de financiamento",
            "mspp_pagetemplateid@odata.bind": f"/mspp_pagetemplates({page_template})",
            "mspp_parentpageid@odata.bind": f"/mspp_webpages({home['mspp_webpageid']})",
            "mspp_publishingstateid@odata.bind": draft_binding,
            "mspp_hiddenfromsitemap": True,
            "mspp_excludefromsearch": True,
        }
        root = self.upsert(
            "mspp_webpage", "FSI - Renegociacao (root)", {**common, "mspp_isroot": True}
        )
        localized = self.upsert("mspp_webpage", "FSI - Renegociacao (pt-BR)", {
            **common,
            "mspp_isroot": False,
            "mspp_rootwebpageid@odata.bind": f"/mspp_webpages({root})",
            "mspp_webpagelanguageid@odata.bind": f"/mspp_websitelanguages({language})",
        })
        self.remove_generated_localized(root, language, draft["mspp_publishingstateid"])
        localizations = [row for row in self.rows("mspp_webpage", (
            f"_mspp_rootwebpageid_value eq {root} and mspp_isroot eq false"
        )) if row.get("_mspp_webpagelanguageid_value") == language]
        if len(localizations) != 1 or localizations[0]["mspp_webpageid"] != localized:
            raise RuntimeError("The dedicated page must have exactly one active localization")
        if enable_chat:
            self.prepare_chat_sdk(root, language, page_type, draft_binding)
        print("Prepared and read back in Draft, not published:", root, localized, flush=True)

    def prepare_chat_sdk(self, root, language, page_type, draft_binding):
        for row in self.rows("mspp_webfile", "mspp_name eq 'FSI - Renegociacao Chat (js)'"):
            component = self.client.records.retrieve("powerpagecomponent", row["mspp_webfileid"])
            if (row.get("mspp_summary") != MARKER or component.get("filecontent") or
                    row.get("_mspp_parentpageid_value") != root or
                    row.get("_mspp_publishingstateid_value") != draft_binding.split("(")[-1].rstrip(")")):
                raise RuntimeError("Refusing to remove a non-empty or unrelated legacy chat upload")
            self.save_before("mspp_webfile", row)
            self.client.records.delete("mspp_webfile", row["mspp_webfileid"])
        includes = []
        for index, source in enumerate(sdk_sources(SDK_PATH.read_text(encoding="utf-8")), 1):
            name = f"{SDK_TEMPLATE} - Part {index}"
            self.upsert("mspp_webtemplate", name, {
                "mspp_source": source, "mspp_mimetype": "application/javascript",
            })
            includes.append("{% include '" + name + "' %}")
        template = self.upsert("mspp_webtemplate", SDK_TEMPLATE, {
            "mspp_source": SDK_MARKER + (ROOT / "pages" / "renegociacao" / "src" / "chat.html")
                .read_text(encoding="utf-8").replace(
                    '<script src="chat.js/" defer></script>',
                    "<script>new Function(" + "".join(includes) + ")();</script>",
                ),
            "mspp_mimetype": "text/html",
        })
        page_template = self.upsert("mspp_pagetemplate", SDK_TEMPLATE, {
            "mspp_type": page_type, "mspp_usewebsiteheaderandfooter": False,
            "mspp_webtemplateid@odata.bind": f"/mspp_webtemplates({template})",
        })
        common = {
            "mspp_partialurl": "chat-app",
            "mspp_parentpageid@odata.bind": f"/mspp_webpages({root})",
            "mspp_pagetemplateid@odata.bind": f"/mspp_pagetemplates({page_template})",
            "mspp_publishingstateid@odata.bind": draft_binding,
            "mspp_hiddenfromsitemap": True, "mspp_excludefromsearch": True,
        }
        sdk_root = self.upsert("mspp_webpage", SDK_TEMPLATE + " (root)", {
            **common, "mspp_isroot": True,
        })
        self.upsert("mspp_webpage", SDK_TEMPLATE + " (pt-BR)", {
            **common, "mspp_isroot": False,
            "mspp_rootwebpageid@odata.bind": f"/mspp_webpages({sdk_root})",
            "mspp_webpagelanguageid@odata.bind": f"/mspp_websitelanguages({language})",
        })

    def publish(self):
        response = requests.post(API + "/api/renegociacao/profile", json={}, timeout=45)
        if response.status_code != 401 or response.json().get("demonstration") is not True:
            raise RuntimeError("The financial service must be running and reject unauthenticated access")
        published = self.one("mspp_publishingstate", "mspp_name eq 'Published'")
        chat = self.rows("mspp_sitesetting", "mspp_name eq 'FSI/Renegociacao/ChatEnabled'")
        chat_enabled = bool(chat and chat[0].get("mspp_value") == "true")
        names = ["FSI - Renegociacao (root)", "FSI - Renegociacao (pt-BR)"]
        if chat_enabled:
            names += [SDK_TEMPLATE + " (root)", SDK_TEMPLATE + " (pt-BR)"]
        for name in names:
            page = self.one("mspp_webpage", f"mspp_name eq '{name}'")
            self.save_before("mspp_webpage", page)
            payload = {
                "mspp_publishingstateid@odata.bind":
                    f"/mspp_publishingstates({published['mspp_publishingstateid']})",
            }
            self.client.records.update("mspp_webpage", page["mspp_webpageid"], payload)
            self.verify("mspp_webpage", page["mspp_webpageid"], payload)
        print("Published only the renegotiation page; existing navigation and bots preserved.")
        print("https://portalatendimento01.powerappsportals.com/" + SLUG + "/")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=["prepare", "publish"])
    parser.add_argument("--environment", required=True)
    parser.add_argument("--backup-dir", required=True, type=Path)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--source-validated", action="store_true")
    parser.add_argument("--enable-chat", action="store_true")
    args = parser.parse_args()
    if args.environment.rstrip("/") != ORG:
        parser.error("Only the approved CCaaS environment is allowed")
    if args.phase == "publish" and not args.source_validated:
        parser.error("Validate the real demo source before publishing (--source-validated)")
    if not args.apply:
        print("Dry run:", args.phase, SITE, SLUG, "template characters:", len(template_source()))
        return
    dnsfix.install()
    with DataverseClient(
        base_url=ORG, credential=AzureCliCredential(),
        context=_build_operation_context("dv-metadata"),
    ) as client:
        deployment = PageDeployment(client, args.backup_dir)
        if args.phase == "prepare":
            deployment.prepare(enable_chat=args.enable_chat)
        else:
            deployment.publish()


if __name__ == "__main__":
    main()
