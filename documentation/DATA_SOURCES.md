# Data Sources & Collection Methodology

Every data point comes from an official US government source. No web scraping of unofficial sites, no AI estimation.

## Per-State Sources

| State | Source | URL | Method | Date | Rows |
|-------|--------|-----|--------|------|------|
| NJ | NJ ABC Retail License Report | [nj.gov/oag/abc](https://www.njoag.gov/wp-content/uploads/2026/04/RETAIL-LICENSE-REPORT-APRIL-2026.xlsx) | Direct Excel download | April 2026 | 8,815 |
| PA | PLCB License Search Export | [plcbplus.pa.gov](https://plcbplus.pa.gov/pub/Default.aspx?PossePresentation=LicenseSearch) | "CSV Download of All Licenses" button | April 2026 | 20,896 active |
| KY | ABC BELLE Portal | [abcportal.ky.gov](https://abcportal.ky.gov/BELLEExternal) | Reports > All Active Licenses > Excel export | April 2026 | 17,260 alcohol |
| MA | ABCC Active Retail Licenses | [mass.gov](https://www.mass.gov/info-details/abcc-active-licenses) | "ABCC Active Retail Licenses" XLS download | April 2026 | 12,471 |
| TX | Open Data Portal TABC | [data.texas.gov](https://data.texas.gov/dataset/TABCLicenses/kguh-7q9z) | Socrata REST API (paginated JSON) | April 2026 | 77,379 |
| AK | AMCO Local Option PDF | [commerce.alaska.gov](https://www.commerce.alaska.gov/web/amco/AlcoholLocalOption) | PDF download + PyPDF2 extraction | March 2024 | 109 |
| AR | GIS Wet/Dry Areas | [gis.arkansas.gov](https://gis.arkansas.gov/arcgis/rest/services/FEATURESERVICES/Boundaries/FeatureServer/60) | ArcGIS REST API query | June 2025 | 260 |
| MD | ATCC Local Jurisdictions | [atcc.maryland.gov](https://atcc.maryland.gov/resources/local-alcohol-jurisdictions-in-maryland/) | Chrome browser HTML extraction | April 2026 | 25 |
| AL | ABC Wet Cities List | [alabcboard.gov](https://alabcboard.gov/licensing-compliance/wet-cities) | WebFetch of official page | April 2026 | 66 |
| NC | ABC Commission | [abc.nc.gov](https://www.abc.nc.gov/nc-abc-boards-and-stores) | Web research from board directory | April 2026 | 100 |
| MS | DOR ABC | [dor.ms.gov](https://www.dor.ms.gov/abc) | Web research from FAQ + dry county lists | April 2026 | 6 |

## How Rules Were Determined

### Control vs License Status
- **Source:** NABCA control state directory (nabca.org/control-state-directory-and-info)
- **Granularity:** `spirits_control`, `wine_control`, `beer_control`, `wholesale_control`, `retail_control`
- Example: PA controls spirits+wine but not beer. IA controls wholesale but not retail.

### Sunday Sales
- **Source:** Each state's ABC agency website + Park Street guides (parkstreet.com/states/)
- **Verification:** NJ Bergen County Blue Laws verified by calling Total Wine (open Sundays)

### Grocery/Convenience Permissions
- **Source:** State statutes + ABC agency FAQs + actual license roster verification
- **PA correction:** `grocery_wine_allowed` initially False, corrected to True after finding Sheetz/Giant/Wawa hold R licenses via Act 39 wine-to-go

### Quota Rules
- **NJ:** 1 consumption per 3,000, 1 distribution per 7,500 (NJ ABC Handbook p.50-52)
- **PA:** 1 per 3,000 county population (PLCB quota system page)
- **Population:** Census Bureau 2020 API (api.census.gov/data/2020/dec/pl)

### Wet/Dry Status
- **KY:** 109 wet (county-wide vote with quota licenses), 6 moist (city-only wet), 5 dry
- **AL:** 44 wet + 22 moist (wet cities in dry counties) from ABC Board wet cities list
- **AK:** 109 restricted communities from AMCO PDF (86 dry, 23 damp)
