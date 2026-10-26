"""Deliberately bounded OData v4 read surface; reject rather than ignore queries."""

import re
from urllib.parse import urlencode
from xml.sax.saxutils import escape

from .core import ApiError, guid, instant, stamp


CONTRACTS = {
    "Id": ("Id", "Guid"), "Name": ("Number", "String"), "ClientRef": ("ContactId", "String"),
    "ProductRef": ("AnchorId", "String"), "Number": ("Number", "String"),
    "Vehicle": ("Vehicle", "String"), "DebtCents": ("DebtCents", "Int32"),
    "Eligible": ("Eligible", "Boolean"), "PositionDate": ("PositionDate", "DateTimeOffset"),
}
INSTALLMENTS = {
    "Id": ("Id", "Guid"), "Name": ("Number", "String"), "ContractRef": ("ContractId", "String"),
    "ClientRef": ("ContactId", "String"), "Number": ("Number", "Int32"),
    "AmountCents": ("AmountCents", "Int32"), "DueDate": ("DueDate", "DateTimeOffset"),
    "Status": ("Status", "String"),
}
SETS = {"Contracts": CONTRACTS, "Installments": INSTALLMENTS}
CLAUSE = re.compile(
    r"([A-Za-z][A-Za-z0-9]*)\s+eq\s+('(?:[^']|'')*'|[^\s']+)"
)


def unsupported() -> ApiError:
    return ApiError("ODATA_UNSUPPORTED_QUERY", "Only typed equality joined by 'and', $select, $top and $skip are supported.")


def filters(text: str, columns: dict) -> list[tuple[str, object]]:
    if not text:
        return []
    if len(text) > 4096:
        raise unsupported()
    clauses, position = [], 0
    while position < len(text):
        match = CLAUSE.match(text, position)
        if not match or len(clauses) >= 12:
            raise unsupported()
        name, literal = match.groups()
        if name not in columns:
            raise unsupported()
        kind = columns[name][1]
        quoted = literal.startswith("'") and literal.endswith("'")
        try:
            if kind == "String":
                if not quoted:
                    raise ValueError()
                value = literal[1:-1].replace("''", "'")
                if name.endswith("Ref"):
                    value = guid(value)
            elif kind == "Guid":
                if quoted or not re.fullmatch(r"[0-9a-fA-F-]{36}", literal):
                    raise ValueError()
                value = guid(literal)
            elif kind == "Boolean":
                if literal not in ("true", "false"):
                    raise ValueError()
                value = literal == "true"
            elif kind == "Int32":
                if not re.fullmatch(r"-?[0-9]+", literal):
                    raise ValueError()
                value = int(literal)
                if not -2_147_483_648 <= value <= 2_147_483_647:
                    raise ValueError()
            else:
                if not re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:Z|[+-]\d\d:\d\d)", literal):
                    raise ValueError()
                value = instant(literal)
        except (ValueError, ApiError):
            raise unsupported() from None
        clauses.append((name, value))
        position = match.end()
        if position < len(text):
            separator = re.match(r"\s+and\s+", text[position:])
            if not separator:
                raise unsupported()
            position += separator.end()
            if position == len(text):
                raise unsupported()
    return clauses


def metadata() -> str:
    entities = []
    for name, columns in SETS.items():
        props = "".join(
            f'<Property Name="{escape(prop)}" Type="Edm.{kind}" Nullable="{"false" if prop == "Id" else "true"}"/>'
            for prop, (_, kind) in columns.items())
        entities.append(f'<EntityType Name="{name[:-1]}"><Key><PropertyRef Name="Id"/></Key>{props}</EntityType>')
    sets = "".join(f'<EntitySet Name="{name}" EntityType="Demo.{name[:-1]}"/>' for name in SETS)
    return ('<?xml version="1.0" encoding="utf-8"?>'
            '<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">'
            '<edmx:DataServices><Schema Namespace="Demo" xmlns="http://docs.oasis-open.org/odata/ns/edm">'
            + "".join(entities) + f'<EntityContainer Name="Source">{sets}</EntityContainer>'
            '</Schema></edmx:DataServices></edmx:Edmx>')


def serialize(row: dict, columns: dict, selected: set[str] | None = None) -> dict:
    result = {}
    for name, (_, kind) in columns.items():
        if selected is not None and name not in selected and name != "Id":
            continue
        value = row[name]
        if kind == "DateTimeOffset":
            value = stamp(value)
        elif kind in ("Guid", "String"):
            value = str(value)
        elif kind == "Boolean":
            value = bool(value)
        result[name] = value
    return result


def read(repository, path: str, query: dict, root: str) -> tuple[object, str]:
    if query.keys() - {"$filter", "$top", "$skip", "$select"}:
        raise unsupported()
    path = path.strip("/")
    if not path:
        if query:
            raise unsupported()
        return {"@odata.context": root + "/$metadata", "value": [
            {"name": name, "kind": "EntitySet", "url": name} for name in SETS]}, "application/json"
    if path == "$metadata":
        if query:
            raise unsupported()
        return metadata(), "application/xml"
    match = re.fullmatch(r"(Contracts|Installments)(?:\(([0-9a-fA-F-]{36})\))?", path)
    if not match:
        raise ApiError("ODATA_NOT_FOUND", "The OData resource does not exist.", 404)
    entity, key = match.groups()
    columns = SETS[entity]
    selected = None
    if "$select" in query:
        names = query["$select"].split(",")
        if not names or len(names) > len(columns):
            raise unsupported()
        selected = {name.strip() for name in names}
        if len(selected) != len(names) or not selected <= columns.keys():
            raise unsupported()
    clauses = filters(query.get("$filter", ""), columns)
    if key:
        if query.keys() - {"$select"}:
            raise unsupported()
        clauses = [("Id", guid(key))]
        rows = repository.odata(entity, columns, clauses, 0, 1)
        if not rows:
            raise ApiError("ODATA_NOT_FOUND", "The OData resource does not exist.", 404)
        return {"@odata.context": root + f"/$metadata#{entity}/$entity",
                **serialize(rows[0], columns, selected)}, "application/json"
    try:
        top_text, skip_text = query.get("$top", "100"), query.get("$skip", "0")
        if not re.fullmatch(r"[0-9]{1,9}", top_text) or not re.fullmatch(r"[0-9]{1,9}", skip_text):
            raise ValueError()
        top, skip = int(top_text), int(skip_text)
    except (TypeError, ValueError):
        raise unsupported() from None
    # ponytail: bound native-provider queries to 1000; bulk readers use explicit windows.
    if top > 1000:
        raise ApiError("ODATA_QUERY_LIMIT", "Use $top <= 1000 and a contract filter or an explicit $skip window.", 413)
    page_size = top
    rows = repository.odata(entity, columns, clauses, skip, page_size + 1) if top else []
    data = {"@odata.context": root + f"/$metadata#{entity}",
            "value": [serialize(r, columns, selected) for r in rows[:page_size]]}
    if len(rows) > page_size and top > page_size:
        following = {**query, "$skip": str(skip + page_size), "$top": str(top - page_size)}
        data["@odata.nextLink"] = root + "/" + entity + "?" + urlencode(following)
    elif len(rows) > page_size and "$top" not in query:
        following = {**query, "$skip": str(skip + page_size)}
        data["@odata.nextLink"] = root + "/" + entity + "?" + urlencode(following)
    return data, "application/json"
