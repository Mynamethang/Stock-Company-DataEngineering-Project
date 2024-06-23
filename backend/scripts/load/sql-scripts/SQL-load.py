"""INSERT TABLE"""
insert_regions = """
    INSERT INTO regions(region_name, region_local_open, region_local_close)
        SELECT 
            * 
        FROM temp_regions
    ON CONFLICT (region_name) DO NOTHING
"""

insert_industries = """
    INSET INTO industries(industry_name, industry_sector)
        SELECT
            *
        FROM temp_industries
    ON CONFLICT (industry_name, industry_sector) DO NOTHING
"""

insert_sic_industries = """
    INSERT INTO sic_industries(sic_id, sic_industry, sic_sector)
        SELECT
            * 
        FROM temp_sic_industries
    ON CONFLICT (sic_id, sic_industry, sic_sector) DO NOTHING
"""

insert_exchages = """
    INSERT INTO exchanges(exchange_region_id, exchange_name)
        SELECT
            r.regions_id,
            e.primary_exchanges as exchange_name
        FROM temp_exchanges e
        JOIN regions ON e.primary_exchanges  = r.
"""


