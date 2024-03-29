text_base = """
            create table if not exists {} (
            GLOBALEVENTID text,
            SQLDATE text,
            MonthYear text,
            Year text,
            FractionDate text,
            Actor1Code text,
            Actor1Name text,
            Actor1CountryCode text,
            Actor1KnownGroupCode text,
            Actor1EthnicCode text,
            Actor1Religion1Code text,
            Actor1Religion2Code text,
            Actor1Type1Code text,
            Actor1Type2Code text,
            Actor1Type3Code text,
            Actor2Code text,
            Actor2Name text,
            Actor2CountryCode text,
            Actor2KnownGroupCode text,
            Actor2EthnicCode text,
            Actor2Religion1Code text,
            Actor2Religion2Code text,
            Actor2Type1Code text,
            Actor2Type2Code text,
            Actor2Type3Code text,
            IsRootEvent text,
            EventCode text,
            EventBaseCode text,
            EventRootCode text,
            QuadClass text,
            GoldsteinScale text,
            NumMentions text,
            NumSources text,
            NumArticles text,
            AvgTone text,
            Actor1Geo_Type text,
            Actor1Geo_FullName text,
            Actor1Geo_CountryCode text,
            Actor1Geo_ADM1Code text,
            Actor1Geo_ADM2Code text,
            Actor1Geo_Lat text,
            Actor1Geo_Long text,
            Actor1Geo_FeatureID text,
            Actor2Geo_Type text,
            Actor2Geo_FullName text,
            Actor2Geo_CountryCode text,
            Actor2Geo_ADM1Code text,
            Actor2Geo_ADM2Code text,
            Actor2Geo_Lat text,
            Actor2Geo_Long text,
            Actor2Geo_FeatureID text,
            ActionGeo_Type text,
            ActionGeo_FullName text,
            ActionGeo_CountryCode text,
            ActionGeo_ADM1Code text,
            ActionGeo_ADM2Code text,
            ActionGeo_Lat text,
            ActionGeo_Long text,
            ActionGeo_FeatureID text,
            DATEADDED text,
            SOURCEURL text
            );
            """


geom_base = """
            select 
            globaleventid, 
            sqldate, 
            actor1name, 
            actor2name, 
            eventcode::text, 
            goldsteinscale::float,
            numarticles::integer, 
            avgtone::float, 
            sourceurl, 
            actor1geo_lat::float, 
            actor1geo_long::float
            into {}
            from {}
            where actor1geo_lat != '' and actor1geo_long != ''
            """

run_base = """
           create table if not exists {} (
           runtime float
           )
           """
