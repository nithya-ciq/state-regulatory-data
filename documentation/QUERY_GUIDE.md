# Query Guide

## The View: `v_establishment_full`

This Supabase view joins individual licenses + state rules + municipality summary. One query answers most questions.

### "Does Costco sell on Sundays in Edison, NJ?"
```sql
SELECT establishment_name, municipality_name, state_abbr,
  sunday_sales_allowed, wet_dry_status, license_type
FROM regulations_data.v_establishment_full
WHERE establishment_name ILIKE '%costco%' AND municipality_name = 'Edison'
```
**Answer:** sunday_sales_allowed=True, wet_dry_status=wet

### "Can Sheetz in PA sell wine?"
```sql
SELECT establishment_name, grocery_wine_allowed, control_status
FROM regulations_data.v_establishment_full
WHERE establishment_name ILIKE '%sheetz%' AND state_fips = '42'
```
**Answer:** grocery_wine_allowed=True (Act 39)

### "Which KY counties are dry?"
```sql
SELECT municipality_name, wet_dry_status, quota_notes
FROM regulations_data.layer2_municipality_licenses
WHERE state_fips = '21' AND wet_dry_status = 'dry'
```
**Answer:** Breathitt, Clinton, Edmonson, Elliott, Knott

### "How many 7-Elevens sell alcohol in Texas?"
```sql
SELECT COUNT(*), county_name
FROM regulations_data.v_establishment_full
WHERE establishment_name ILIKE '%7-eleven%' AND state_fips = '48'
GROUP BY county_name ORDER BY count DESC
```
**Answer:** 1,272 across TX

### "Is there room for a new bar in Pittsburgh?"
```sql
SELECT municipality_name, is_over_consumption_quota, quota_notes
FROM regulations_data.layer2_municipality_licenses
WHERE municipality_name = 'Pittsburgh' AND state_fips = '42'
```
**Answer:** Over quota (575 bars / 416 max)

## Column Reference

| Column | Source Table | Answers |
|--------|------------|---------|
| `establishment_name` | layer2_individual_licenses | Who is this business? |
| `municipality_name` | layer2_individual_licenses | What city/town? |
| `state_abbr` | dim_states | What state? |
| `license_type` | layer2_individual_licenses | What license? |
| `sunday_sales_allowed` | dim_states | Sunday sales? (state rule) |
| `grocery_beer_allowed` | dim_states | Grocery beer? (state rule) |
| `grocery_wine_allowed` | dim_states | Grocery wine? (state rule) |
| `grocery_liquor_allowed` | dim_states | Grocery spirits? (state rule) |
| `control_status` | dim_states | Control or license state? |
| `spirits_control` | dim_states | State controls spirits? |
| `wet_dry_status` | layer2_municipality_licenses | Wet/dry/moist/damp? |
| `on_premise_allowed` | layer2_municipality_licenses | Can bars operate? |
| `municipality_grocery_sells` | layer2_municipality_licenses | Any store sells alcohol? |
| `population_2020` | layer2_municipality_licenses | Census population |
| `quota_notes` | layer2_municipality_licenses | Quota status |
| `exception_notes` | layer2_municipality_licenses | Special exceptions |
