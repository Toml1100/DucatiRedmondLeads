from flask import Flask, request, jsonify
import os
import re
import json
import html
import base64
import hashlib
import traceback
import requests
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
load_dotenv()

app = Flask(__name__)

AZDO_ORG = os.getenv("AZDO_ORG", "tom0553").strip()
AZDO_PROJECT = os.getenv("AZDO_PROJECT", "Ducati%20Redmond%20Leads").strip()
AZDO_PAT = os.getenv("AZDO_PAT", "").strip()
AZDO_WORK_ITEM_TYPE = os.getenv("AZDO_WORK_ITEM_TYPE", "Issue").strip()


def require_env():
    missing = []
    if not AZDO_ORG:
        missing.append("AZDO_ORG")
    if not AZDO_PROJECT:
        missing.append("AZDO_PROJECT")
    if not AZDO_PAT:
        missing.append("AZDO_PAT")
    if missing:
        raise RuntimeError(f"Missing environment variables: {', '.join(missing)}")


def azdo_headers_json_patch():
    token = base64.b64encode(f":{AZDO_PAT}".encode()).decode()
    return {
        "Authorization": f"Basic {token}",
        "Content-Type": "application/json-patch+json",
    }


def azdo_headers_json():
    token = base64.b64encode(f":{AZDO_PAT}".encode()).decode()
    return {
        "Authorization": f"Basic {token}",
        "Content-Type": "application/json",
    }


def extract_possible_xml(text: str) -> str:
    if not text:
        return ""

    match = re.search(r"<adf>.*?</adf>", text, flags=re.DOTALL | re.IGNORECASE)
    if not match:
        return ""

    xml_text = match.group(0)
    xml_text = xml_text.replace("\r\n", "\n").replace("\r", "\n")

    xml_text = re.sub(
        r"</comments>\s*<https?://[^>\n]+>",
        "</comments>",
        xml_text,
        flags=re.IGNORECASE,
    )
    xml_text = re.sub(
        r"<https?://[^>\n]+>",
        "",
        xml_text,
        flags=re.IGNORECASE,
    )
    xml_text = re.sub(
        r"(</comments>)https?://[^\s<]+",
        r"\1",
        xml_text,
        flags=re.IGNORECASE,
    )

    return xml_text.strip()


def safe_find_text(node, path: str) -> str:
    found = node.find(path)
    if found is not None and found.text:
        return found.text.strip()
    return ""


def parse_adf_xml(xml_text: str) -> dict:
    if not xml_text:
        return {}

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        print("----- SANITIZED XML THAT FAILED -----")
        print(xml_text)
        raise RuntimeError(f"ADF XML parse failed: {e}")

    lead = {
        "request_date": safe_find_text(root, "./prospect/requestdate"),
        "customer_name": safe_find_text(root, "./prospect/customer/contact/name"),
        "customer_email": safe_find_text(root, "./prospect/customer/contact/email"),
        "customer_phone": safe_find_text(root, "./prospect/customer/contact/phone"),
        "vehicle_year": safe_find_text(root, "./prospect/vehicle/year"),
        "vehicle_make": safe_find_text(root, "./prospect/vehicle/make"),
        "vehicle_model": safe_find_text(root, "./prospect/vehicle/model"),
        "vehicle_comments": safe_find_text(root, "./prospect/vehicle/comments"),
        "customer_comments": safe_find_text(root, "./prospect/customer/comments"),
        "vendor_name": safe_find_text(root, "./prospect/vendor/vendorname"),
        "vendor_phone": safe_find_text(root, "./prospect/vendor/contact/phone"),
        "source_name": safe_find_text(root, "./prospect/provider/name"),
        "source_service": safe_find_text(root, "./prospect/provider/service"),
    }

    comments = lead.get("customer_comments", "") or ""

    lead_type_match = re.search(r"Lead Type:\s*(.+)", comments, re.IGNORECASE)
    receive_offers_match = re.search(r"Receive Offers:\s*(.+)", comments, re.IGNORECASE)
    item_match = re.search(r"item:\s*(.+)", comments, re.IGNORECASE)
    interest_match = re.search(
        r"comments:\s*(.+?)(?:\nitem:|\Z)",
        comments,
        re.IGNORECASE | re.DOTALL,
    )

    lead["lead_type"] = lead_type_match.group(1).strip() if lead_type_match else ""
    lead["receive_offers"] = receive_offers_match.group(1).strip() if receive_offers_match else ""
    lead["item"] = item_match.group(1).strip() if item_match else ""
    lead["customer_message"] = interest_match.group(1).strip() if interest_match else comments.strip()

    return lead


def build_title(lead: dict, subject: str) -> str:
    name = lead.get("customer_name", "").strip()
    lead_type = lead.get("lead_type", "").strip() or subject.strip() or "Lead"
    bike = " ".join(
        x for x in [
            lead.get("vehicle_year", ""),
            lead.get("vehicle_make", ""),
            lead.get("vehicle_model", ""),
        ] if x
    ).strip()

    parts = [lead_type]
    if name:
        parts.append(name)
    if bike:
        parts.append(bike)

    return " - ".join(parts)[:255]


def build_description(postmark_data: dict, lead: dict, xml_text: str, dedupe_key: str) -> str:
    def esc(value: str) -> str:
        return html.escape(value or "")

    bike = " ".join(
        x for x in [
            lead.get("vehicle_year", ""),
            lead.get("vehicle_make", ""),
            lead.get("vehicle_model", ""),
        ] if x
    ).strip()

    from_email = (postmark_data.get("FromFull") or {}).get("Email") or postmark_data.get("From", "")
    subject = postmark_data.get("Subject", "")
    message_id = postmark_data.get("MessageID", "")

    return (
        f"<p><b>Name:</b> {esc(lead.get('customer_name', ''))}</p>"
        f"<p><b>Email:</b> {esc(lead.get('customer_email', ''))}</p>"
        f"<p><b>Phone:</b> {esc(lead.get('customer_phone', ''))}</p>"
        f"<p><b>Vehicle:</b> {esc(bike)}</p>"
        f"<p><b>Inventory URL / Vehicle Comments:</b> {esc(lead.get('vehicle_comments', ''))}</p>"
        f"<p><b>Request Date:</b> {esc(lead.get('request_date', ''))}</p>"
        f"<p><b>Lead Type:</b> {esc(lead.get('lead_type', ''))}</p>"
        f"<p><b>Customer Message:</b> {esc(lead.get('customer_message', ''))}</p>"
        f"<p><b>Item:</b> {esc(lead.get('item', ''))}</p>"
        f"<p><b>Receive Offers:</b> {esc(lead.get('receive_offers', ''))}</p>"
        f"<p><b>Vendor:</b> {esc(lead.get('vendor_name', ''))}</p>"
        f"<p><b>Vendor Phone:</b> {esc(lead.get('vendor_phone', ''))}</p>"
        f"<p><b>Source:</b> {esc(lead.get('source_name', ''))} / {esc(lead.get('source_service', ''))}</p>"
        f"<p><b>Dedupe Key:</b> {esc(dedupe_key)}</p>"
        f"<hr>"
        f"<p><b>Inbound Email From:</b> {esc(from_email)}</p>"
        f"<p><b>Email Subject:</b> {esc(subject)}</p>"
        f"<p><b>Postmark Message ID:</b> {esc(message_id)}</p>"
        f"<hr>"
        f"<pre>{esc(xml_text)}</pre>"
    )


def make_dedupe_key(lead: dict) -> str:
    """
    Stable key based on the lead itself, not Postmark message ID.
    This prevents duplicates even if the same lead is forwarded more than once.
    """
    pieces = [
        (lead.get("request_date", "") or "").strip().lower(),
        (lead.get("customer_email", "") or "").strip().lower(),
        (lead.get("customer_phone", "") or "").strip(),
        (lead.get("lead_type", "") or "").strip().lower(),
        (lead.get("item", "") or "").strip().lower(),
        (lead.get("vehicle_year", "") or "").strip(),
        (lead.get("vehicle_make", "") or "").strip().lower(),
        (lead.get("vehicle_model", "") or "").strip().lower(),
    ]

    raw = "|".join(pieces)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
    return f"leadkey_{digest}"


def build_tags(lead: dict, dedupe_key: str) -> str:
    tags = ["lead", "dealerspike", dedupe_key]

    lead_type = (lead.get("lead_type", "") or "").strip()
    if lead_type:
        tags.append(lead_type)

    vehicle_make = (lead.get("vehicle_make", "") or "").strip()
    if vehicle_make:
        tags.append(vehicle_make)

    vehicle_model = (lead.get("vehicle_model", "") or "").strip()
    if vehicle_model:
        tags.append(vehicle_model)

    # remove empties and duplicates while preserving order
    seen = set()
    cleaned = []
    for tag in tags:
        t = tag.strip()
        if t and t not in seen:
            seen.add(t)
            cleaned.append(t)

    return ";".join(cleaned)


def find_existing_work_item_by_dedupe_key(dedupe_key: str):
    require_env()

    wiql_url = f"https://dev.azure.com/{AZDO_ORG}/{AZDO_PROJECT}/_apis/wit/wiql?api-version=7.1"

    wiql = {
        "query": (
            "SELECT [System.Id], [System.Title] "
            "FROM WorkItems "
            f"WHERE [System.TeamProject] = @project "
            f"AND [System.Tags] CONTAINS '{dedupe_key}' "
            "ORDER BY [System.ChangedDate] DESC"
        )
    }

    print("----- AZDO WIQL REQUEST -----")
    print(wiql_url)
    print(json.dumps(wiql, indent=2))

    response = requests.post(
        wiql_url,
        headers=azdo_headers_json(),
        json=wiql,
        timeout=30,
    )

    print("----- AZDO WIQL RESPONSE -----")
    print(response.status_code)
    print(response.text[:4000])

    response.raise_for_status()
    data = response.json()

    work_items = data.get("workItems", [])
    if not work_items:
        return None

    return work_items[0]


def create_work_item(title: str, description_html: str, tags: str) -> dict:
    require_env()

    url = (
        f"https://dev.azure.com/{AZDO_ORG}/{AZDO_PROJECT}"
        f"/_apis/wit/workitems/${AZDO_WORK_ITEM_TYPE}?api-version=7.1"
    )

    body = [
        {
            "op": "add",
            "path": "/fields/System.Title",
            "value": title,
        },
        {
            "op": "add",
            "path": "/fields/System.Description",
            "value": description_html,
        },
        {
            "op": "add",
            "path": "/fields/System.Tags",
            "value": tags,
        },
    ]

    print("----- AZDO CREATE REQUEST URL -----")
    print(url)
    print("----- AZDO CREATE REQUEST BODY -----")
    print(json.dumps(body, indent=2)[:4000])

    response = requests.post(url, headers=azdo_headers_json_patch(), json=body, timeout=30)

    print("----- AZDO CREATE RESPONSE -----")
    print(response.status_code)
    print(response.text[:4000])

    response.raise_for_status()
    return response.json()


@app.route("/webhooks/postmark", methods=["POST"])
def postmark_inbound():
    try:
        data = request.get_json(force=True)

        print("----- INBOUND EMAIL -----")
        print(json.dumps(data, indent=2)[:12000])

        text_body = data.get("TextBody", "") or ""
        subject = data.get("Subject", "") or ""

        xml_text = extract_possible_xml(text_body)
        if not xml_text:
            raise RuntimeError("No <adf>...</adf> XML block found in TextBody")

        lead = parse_adf_xml(xml_text)
        if not lead:
            raise RuntimeError("Failed to parse ADF/XML lead")

        dedupe_key = make_dedupe_key(lead)
        existing = find_existing_work_item_by_dedupe_key(dedupe_key)

        if existing:
            result = {
                "ok": True,
                "duplicate": True,
                "message": "Matching lead already exists; skipped creating a new work item.",
                "existing_work_item_id": existing.get("id"),
                "dedupe_key": dedupe_key,
                "lead": lead,
            }
            print("----- DUPLICATE DETECTED -----")
            print(json.dumps(result, indent=2))
            return jsonify(result), 200

        title = build_title(lead, subject)
        tags = build_tags(lead, dedupe_key)
        description_html = build_description(data, lead, xml_text, dedupe_key)

        work_item = create_work_item(title, description_html, tags)

        result = {
            "ok": True,
            "duplicate": False,
            "work_item_id": work_item.get("id"),
            "work_item_url": work_item.get("url"),
            "title": title,
            "tags": tags,
            "dedupe_key": dedupe_key,
            "lead": lead,
        }

        print("----- AZDO WORK ITEM CREATED -----")
        print(json.dumps(result, indent=2))

        return jsonify(result), 200

    except Exception as exc:
        print("----- ERROR -----")
        print(str(exc))
        traceback.print_exc()
        return jsonify({
            "ok": False,
            "error": str(exc),
        }), 500


@app.route("/", methods=["GET"])
def healthcheck():
    return jsonify({"ok": True, "service": "postmark-azure-devops-webhook"}), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port, debug=False)