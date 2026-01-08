USE Airbnb;
GO
------------------------------
WITH UniqueHosts AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY host_id          -- Group by Host ID
            ORDER BY (SELECT NULL)        -- We don't care which row we pick, just pick one
        ) as RowNum
    FROM Stage.Staging_Raw_Data
    WHERE TRY_CAST(host_id AS INT) IS NOT NULL -- Safety check: ignore garbage IDs
)

INSERT INTO DW.DimHost (
    HostID,
    HostExperienceYears,
    HostLocation,
    HostListingsCount,
    HostTotalListingsCount,
    HostIsSuperHost,
    HostIdentityVerified,
    HostHasProfilePic,
    HostResponseTime,
    HostResponseRate,
    HostAcceptanceRate,
    HostSince,
    CalculatedHostListingsCount,
    CalculatedHostListingsCountEntireHomes,
    CalculatedHostListingsCountPrivateRooms,
    CalculatedHostListingsCountSharedRooms
)
SELECT 
    TRY_CAST(host_id AS INT),
    TRY_CAST(host_experience_years AS INT),

    NULLIF(TRIM(host_location), ''),

    TRY_CAST(host_listings_count AS INT),
    TRY_CAST(host_total_listings_count AS INT),

    -- 5. Booleans (Handling 't'/'f', 'true'/'false', '1'/'0')
    CASE 
        WHEN LOWER(host_is_superhost) IN ('t', 'true') THEN 1
        ELSE 0 
    END,

    CASE 
        WHEN LOWER(host_identity_verified) IN ('t', 'true') THEN 1
        ELSE 0 
    END,

    CASE 
        WHEN LOWER(host_has_profile_pic) IN ('t', 'true') THEN 1
        ELSE 0 
    END,

    NULLIF(TRIM(host_response_time), ''),

    TRY_CAST(host_response_rate AS DECIMAL(5,2)),
    TRY_CAST(host_acceptance_rate AS DECIMAL(5,2)),

    TRY_CAST(host_since AS DATE),

    -- 9. Small Integers
    TRY_CAST(calculated_host_listings_count AS SMALLINT),
    TRY_CAST(calculated_host_listings_count_entire_homes AS SMALLINT),
    TRY_CAST(calculated_host_listings_count_private_rooms AS SMALLINT),
    TRY_CAST(calculated_host_listings_count_shared_rooms AS SMALLINT)

FROM UniqueHosts
WHERE RowNum = 1;

-------------------------------

INSERT INTO DW.DimLocation (
    Region,
    NeighbourhoodCleansed,
    Latitude,
    Longitude
)
SELECT DISTINCT 
    NULLIF(TRIM(region), ''),

    NULLIF(TRIM(neighbourhood_cleansed), ''),

    TRY_CAST(latitude AS DECIMAL(9, 6)),
    TRY_CAST(longitude AS DECIMAL(9, 6))

FROM Stage.Staging_Raw_Data
WHERE 
    -- Only insert if we actually have coordinates
    TRY_CAST(latitude AS DECIMAL(9, 6)) IS NOT NULL 
    AND 
    TRY_CAST(longitude AS DECIMAL(9, 6)) IS NOT NULL;

----------------------

INSERT INTO DW.DimProperty (
    PropertyID,
    PropertyType,
    RoomType,
    Accomodates,
    Bathrooms,
    HasSharedBathroom,
    Bedrooms,
    Beds,
    InstantBookable,
    MinimumNights,
    MaximumNights,
    AverageMinimumNights,
    AverageMaximumNights,
    FirstReview,
    LastReview
)
SELECT 
    TRY_CAST(id AS VARCHAR(100)),
    
    NULLIF(TRIM(property_type), ''),
    NULLIF(TRIM(room_type), ''),

    TRY_CAST(accommodates AS TINYINT),

    TRY_CAST(bathrooms AS DECIMAL(5,2)),

    CASE 
        WHEN LOWER(is_bathroom_shared) IN ('t', 'true') THEN 1
        ELSE 0
    END,

    TRY_CAST(bedrooms AS TINYINT),
    TRY_CAST(beds AS TINYINT),

    CASE 
        WHEN LOWER(instant_bookable) IN ('t', 'true') THEN 1
        ELSE 0 
    END,

    TRY_CAST(minimum_nights AS SMALLINT),
    TRY_CAST(maximum_nights AS SMALLINT),

    TRY_CAST(minimum_nights_avg_ntm AS DECIMAL(6,2)),
    TRY_CAST(maximum_nights_avg_ntm AS DECIMAL(6,2)),

    TRY_CAST(first_review AS DATE),
    TRY_CAST(last_review AS DATE)

FROM Stage.Staging_Raw_Data

------------------------------------
INSERT INTO DW.DimDate (
    DateID,
    ScrapedDate,
    ScrapedMonth,
    ScrapedYear
)
SELECT DISTINCT
    -- 1. Create a "Smart Key" ID (Format: YYYYMMDD)
    CAST(CONVERT(VARCHAR(8), TRY_CAST(last_scraped AS DATE), 112) AS INT) AS DateID,

    -- 2. The actual date object
    TRY_CAST(last_scraped AS DATE),

    -- 3. The Month Name (e.g., 'September')
    DATENAME(MONTH, TRY_CAST(last_scraped AS DATE)),

    -- 4. The Year (e.g., 2023)
    YEAR(TRY_CAST(last_scraped AS DATE))

FROM Stage.Staging_Raw_Data
WHERE TRY_CAST(last_scraped AS DATE) IS NOT NULL;

-------------------------------

INSERT INTO DW.FactListing (
    DateID,
    PropertyID,
    HostID,
    LocationID,
    Price,
    PricePerPerson,
    Availability30,
    Availability60,
    Availability90,
    Availability365,
    AvailabilityEOY,
    OccupancyRate365,
    EstimatedOccupancyL365D,
    EstimatedRevenueL365D,
    HasAvailability,
    NumberOfReviews,
    NumberOfReviewsLTM,
    NumberOfReviewsL30D,
    NumberOfReviewsLastYear,
    ReviewsPerMonth,
    ReviewScoresRating,
    ReviewScoresAccuracy,
    ReviewScoresCleanliness,
    ReviewScoresCheckIN,
    ReviewScoresCommunication,
    ReviewScoresLocation,
    ReviewScoresValue,
    RatingWeighted
)
SELECT 
    D.DateID,
    P.PropertyID,
    H.HostID,
    L.LocationID,

    -- 5. Price
    TRY_CAST(S.price AS INT),
    TRY_CAST(S.price_per_person AS DECIMAL(9,2)),
    TRY_CAST(S.availability_30 AS TINYINT),
    TRY_CAST(S.availability_60 AS TINYINT),
    TRY_CAST(S.availability_90 AS TINYINT),
    TRY_CAST(S.availability_365 AS SMALLINT),
    TRY_CAST(S.availability_eoy AS TINYINT),
    TRY_CAST(S.occupancy_rate_365 AS DECIMAL(3,2)),
    TRY_CAST(S.estimated_occupancy_l365d AS SMALLINT),
    TRY_CAST(S.estimated_revenue_l365d AS INT),

    CASE WHEN LOWER(S.has_availability_clean) IN ('t', 'true') THEN 1 ELSE 0 END,

    TRY_CAST(S.number_of_reviews AS SMALLINT),
    TRY_CAST(S.number_of_reviews_ltm AS SMALLINT),
    TRY_CAST(S.number_of_reviews_l30d AS TINYINT),
    TRY_CAST(S.number_of_reviews_ly AS TINYINT),
    TRY_CAST(S.reviews_per_month AS DECIMAL(5,2)),

    TRY_CAST(S.review_scores_rating AS DECIMAL(3,2)),
    TRY_CAST(S.review_scores_accuracy AS DECIMAL(3,2)),
    TRY_CAST(S.review_scores_cleanliness AS DECIMAL(3,2)),
    TRY_CAST(S.review_scores_checkin AS DECIMAL(3,2)),
    TRY_CAST(S.review_scores_communication AS DECIMAL(3,2)),
    TRY_CAST(S.review_scores_location AS DECIMAL(3,2)),
    TRY_CAST(S.review_scores_value AS DECIMAL(3,2)),

    TRY_CAST(S.rating_weighted AS DECIMAL(7,2))

FROM Stage.Staging_Raw_Data S

INNER JOIN DW.DimDate D
    ON TRY_CAST(S.last_scraped AS DATE) = D.ScrapedDate

INNER JOIN DW.DimProperty P
    ON TRY_CAST(S.id AS VARCHAR(100)) = P.PropertyID

LEFT JOIN DW.DimHost H
    ON TRY_CAST(S.host_id AS INT) = H.HostID

LEFT JOIN DW.DimLocation L
    ON TRY_CAST(S.latitude AS DECIMAL(9,6)) = L.Latitude
    AND TRY_CAST(S.longitude AS DECIMAL(9,6)) = L.Longitude

WHERE P.PropertyID IS NOT NULL
  AND D.DateID IS NOT NULL;