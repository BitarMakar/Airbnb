USE Airbnb;
GO

CREATE TABLE DW.DimHost (
    HostID INT PRIMARY KEY,
    HostExperienceYears INT,
    HostLocation NVARCHAR(100),
    HostListingsCount INT,
    HostTotalListingsCount INT,
    HostIsSuperHost BIT,
    HostIdentityVerified BIT,
    HostHasProfilePic BIT,
    HostResponseTime NVARCHAR(100),
    HostResponseRate DECIMAL(5,2), 
    HostAcceptanceRate DECIMAL(5,2),
    HostSince DATE,
    CalculatedHostListingsCount SMALLINT,
    CalculatedHostListingsCountEntireHomes SMALLINT,
    CalculatedHostListingsCountPrivateRooms SMALLINT,
    CalculatedHostListingsCountSharedRooms SMALLINT
);

CREATE TABLE DW.DimLocation (
    LocationID INT IDENTITY(1,1) PRIMARY KEY,
    Region NVARCHAR(100),
    NeighbourhoodCleansed NVARCHAR(100),
    Latitude DECIMAL(9, 6),
    Longitude DECIMAL(9, 6)
);

CREATE TABLE DW.DimProperty (
    PropertyID NVARCHAR(100) PRIMARY KEY,
    PropertyType NVARCHAR(100),
    RoomType NVARCHAR(100),
    Accomodates TINYINT,
    Bathrooms DECIMAL(5,2),
    HasSharedBathroom BIT,
    Bedrooms TINYINT,
    Beds TINYINT,
    InstantBookable BIT,
    MinimumNights SMALLINT,
    MaximumNights SMALLINT,
    AverageMinimumNights DECIMAL(6,2),
    AverageMaximumNights DECIMAL(6,2),
    FirstReview DATE,
    LastReview DATE
);

CREATE TABLE DW.DimDate (
    DateID INT PRIMARY KEY,
    ScrapedDate DATE,
    ScrapedMonth VARCHAR(10),
    ScrapedYear SMALLINT
);

CREATE TABLE DW.FactListing(
    DateID INT NOT NULL,
    PropertyID NVARCHAR(100) NOT NULL,
    HostID INT,
    LocationID INT,
    Price INT,
    PricePerPerson DECIMAL(9,2),
    Availability30 TINYINT,
    Availability60 TINYINT,
    Availability90 TINYINT,
    Availability365 SMALLINT,
    AvailabilityEOY TINYINT,
    OccupancyRate365 Decimal(3,2),
    EstimatedOccupancyL365D SMALLINT,
    EstimatedRevenueL365D INT,
    HasAvailability BIT,
    NumberOfReviews SMALLINT,
    NumberOfReviewsLTM SMALLINT,
    NumberOfReviewsL30D TINYINT,
    NumberOfReviewsLastYear SMALLINT,
    ReviewsPerMonth DECIMAL(5,2),
    ReviewScoresRating DECIMAL(3,2),
    ReviewScoresAccuracy DECIMAL(3,2),
    ReviewScoresCleanliness DECIMAL(3,2),
    ReviewScoresCheckIN DECIMAL(3,2),
    ReviewScoresCommunication DECIMAL(3,2),
    ReviewScoresLocation DECIMAL(3,2),
    ReviewScoresValue DECIMAL(3,2),
    RatingWeighted DECIMAL(7,2),

    CONSTRAINT PK_FactListing PRIMARY KEY (PropertyID, DateID),
    CONSTRAINT FK_Fact_Host FOREIGN KEY (HostID)
        REFERENCES DW.DimHost(HostID),
    CONSTRAINT FK_Fact_Date FOREIGN KEY (DateID) 
        REFERENCES DW.DimDate(DateID),
    CONSTRAINT FK_Fact_Location FOREIGN KEY (LocationID) 
        REFERENCES DW.DimLocation(LocationID),
    CONSTRAINT FK_Fact_Property FOREIGN KEY (PropertyID) 
        REFERENCES DW.DimProperty(PropertyID)
);

