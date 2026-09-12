"""Officially assigned ISO 3166-1 alpha-2 country codes.

The published schema warns that format alone is not sufficient: 'UK' and 'EN'
both match ^[A-Z]{2}$ but are not assigned codes, and both appear in the
dataset as planted defects. A regex check therefore passes them. Validation
needs membership in the assigned set.

Held as a literal rather than pulled from a library (pycountry, iso3166) on
purpose: one fewer dependency, no network or data-file load at import, and the
set is auditable in review. The trade-off is that it must be refreshed when the
standard changes, which is rare but real - codes are occasionally added
(for example XK for Kosovo is still only user-assigned) or transitioned.

Includes the 249 officially assigned codes. Excludes user-assigned,
exceptionally reserved, and transitionally reserved codes.
"""

from __future__ import annotations

ISO_3166_1_ALPHA_2: frozenset[str] = frozenset(
    {
        "AD", "AE", "AF", "AG", "AI", "AL", "AM", "AO", "AQ", "AR",
        "AS", "AT", "AU", "AW", "AX", "AZ",
        "BA", "BB", "BD", "BE", "BF", "BG", "BH", "BI", "BJ", "BL",
        "BM", "BN", "BO", "BQ", "BR", "BS", "BT", "BV", "BW", "BY", "BZ",
        "CA", "CC", "CD", "CF", "CG", "CH", "CI", "CK", "CL", "CM",
        "CN", "CO", "CR", "CU", "CV", "CW", "CX", "CY", "CZ",
        "DE", "DJ", "DK", "DM", "DO", "DZ",
        "EC", "EE", "EG", "EH", "ER", "ES", "ET",
        "FI", "FJ", "FK", "FM", "FO", "FR",
        "GA", "GB", "GD", "GE", "GF", "GG", "GH", "GI", "GL", "GM",
        "GN", "GP", "GQ", "GR", "GS", "GT", "GU", "GW", "GY",
        "HK", "HM", "HN", "HR", "HT", "HU",
        "ID", "IE", "IL", "IM", "IN", "IO", "IQ", "IR", "IS", "IT",
        "JE", "JM", "JO", "JP",
        "KE", "KG", "KH", "KI", "KM", "KN", "KP", "KR", "KW", "KY", "KZ",
        "LA", "LB", "LC", "LI", "LK", "LR", "LS", "LT", "LU", "LV", "LY",
        "MA", "MC", "MD", "ME", "MF", "MG", "MH", "MK", "ML", "MM",
        "MN", "MO", "MP", "MQ", "MR", "MS", "MT", "MU", "MV", "MW",
        "MX", "MY", "MZ",
        "NA", "NC", "NE", "NF", "NG", "NI", "NL", "NO", "NP", "NR",
        "NU", "NZ",
        "OM",
        "PA", "PE", "PF", "PG", "PH", "PK", "PL", "PM", "PN", "PR",
        "PS", "PT", "PW", "PY",
        "QA",
        "RE", "RO", "RS", "RU", "RW",
        "SA", "SB", "SC", "SD", "SE", "SG", "SH", "SI", "SJ", "SK",
        "SL", "SM", "SN", "SO", "SR", "SS", "ST", "SV", "SX", "SY", "SZ",
        "TC", "TD", "TF", "TG", "TH", "TJ", "TK", "TL", "TM", "TN",
        "TO", "TR", "TT", "TV", "TW", "TZ",
        "UA", "UG", "UM", "US", "UY", "UZ",
        "VA", "VC", "VE", "VG", "VI", "VN", "VU",
        "WF", "WS",
        "YE", "YT",
        "ZA", "ZM", "ZW",
    }
)

# Codes that match the pattern but are NOT assigned. Kept only to make the
# distinction explicit in tests and code review; not used for validation.
KNOWN_INVALID_LOOKALIKES: frozenset[str] = frozenset({"UK", "EN", "XX", "ZZ", "EU"})
