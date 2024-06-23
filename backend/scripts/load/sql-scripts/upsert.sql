
INSERT INTO regions(region_name, region_local_open, region_local_close)
    SELECT 
        * 
    FROM temp_regions
ON CONFLICT (region_name) DO NOTHING;

INSERT INTO industries(industry_name, industry_sector)
    SELECT
        *
    FROM temp_industries
ON CONFLICT (industry_name, industry_sector) DO NOTHING;

INSERT INTO sic_industries(sic_id, sic_industry, sic_sector)
    SELECT
        * 
    FROM temp_sic_industries
ON CONFLICT (sic_industry, sic_sector) DO NOTHING;

INSERT INTO exchanges(exchange_region_id, exchange_name)
    SELECT
        r.region_id,
        e.exchange_name
    FROM temp_exchanges e
    JOIN regions r ON r.region_name = e.region
ON CONFLICT (exchange_name) DO NOTHING;

INSERT INTO companies(company_id, company_exchange_id, company_industry_id, company_sic_id, company_name, company_ticket, company_is_delisted, company_category, company_currency, company_location)
    SELECT 
        c.company_id,
        e.exchange_id as company_exchange_id,
        i.industry_id as company_industry_id,
        c.company_sic_id,
        c.company_name,
        c.company_ticket,
        c.company_is_delisted,
        c.company_category,
        c.company_currency,
        c.company_location
    FROM temp_companies c
    JOIN exchanges e ON e.exchange_name = c.company_exchange
    JOIN industries i ON i.industry_name = c.company_industry AND i.industry_sector = c.company_sector
ON CONFLICT (company_ticket, company_is_delisted) DO NOTHING