SELECT 
    *
FROM
    dubizzle_car_dataset.dubizzle_cars_dataset;
SELECT 
    COUNT(*) AS missing
FROM
    dubizzle_cars_dataset
WHERE
    seller_type IS NULL;

CREATE TABLE cars_db_staging LIKE dubizzle_cars_dataset;
SELECT 
    *
FROM
    cars_db_staging;

INSERT cars_db_staging SELECT * from dubizzle_cars_dataset;

-- Remove Duplicates:

SELECT *, ROW_NUMBER() OVER (
PARTITION BY price, brand, model, trim, kilometers, `year`, vehicle_age_years, regional_specs, doors, body_type, fuel_type, seating_capacity, transmission_type, engine_capacity_cc, horsepower, no_of_cylinders, exterior_color, interior_color, warranty, address, country, city, area_name, location_name, latitude, longitude, seller_type
) as row_num FROM cars_db_staging;

WITH duplicate_row AS 
(
SELECT *, ROW_NUMBER() OVER (
PARTITION BY price, brand, model, trim, kilometers, `year`, vehicle_age_years, regional_specs, doors, body_type, fuel_type, seating_capacity, transmission_type, engine_capacity_cc, horsepower, no_of_cylinders, exterior_color, interior_color, warranty, address, country, city, area_name, location_name, latitude, longitude, seller_type
) as row_num FROM cars_db_staging
)
SELECT * FROM duplicate_row WHERE row_num >1; 

WITH duplicate_row AS 
(
SELECT *, ROW_NUMBER() OVER (
PARTITION BY price, brand, model, trim, kilometers, `year`, vehicle_age_years, regional_specs, doors, body_type, fuel_type, seating_capacity, transmission_type, engine_capacity_cc, horsepower, no_of_cylinders, exterior_color, interior_color, warranty, address, country, city, area_name, location_name, latitude, longitude, seller_type
) as row_num FROM cars_db_staging
)
SELECT count(*) FROM duplicate_row WHERE row_num >1; 

WITH duplicate_row AS 
(
SELECT *, ROW_NUMBER() OVER (
PARTITION BY price, brand, model, trim, kilometers, `year`, vehicle_age_years, regional_specs, doors, body_type, fuel_type, seating_capacity, transmission_type, engine_capacity_cc, horsepower, no_of_cylinders, exterior_color, interior_color, warranty, address, country, city, area_name, location_name, latitude, longitude, seller_type
) as row_num FROM cars_db_staging
)
DELETE FROM duplicate_row WHERE row_num >1;-- The target table duplicate_roe of the DELETE is not updatable (So, we can create new table like below than delete)

CREATE TABLE `cars_db_staging2` (
    `price` INT DEFAULT NULL,
    `brand` TEXT,
    `model` TEXT,
    `trim` TEXT,
    `kilometers` INT DEFAULT NULL,
    `year` INT DEFAULT NULL,
    `vehicle_age_years` INT DEFAULT NULL,
    `regional_specs` TEXT,
    `doors` INT DEFAULT NULL,
    `body_type` TEXT,
    `fuel_type` TEXT,
    `seating_capacity` INT DEFAULT NULL,
    `transmission_type` TEXT,
    `engine_capacity_cc` TEXT,
    `horsepower` TEXT,
    `no_of_cylinders` INT DEFAULT NULL,
    `exterior_color` TEXT,
    `interior_color` TEXT,
    `warranty` TEXT,
    `address` TEXT,
    `country` TEXT,
    `city` TEXT,
    `area_name` TEXT,
    `location_name` TEXT,
    `latitude` DOUBLE DEFAULT NULL,
    `longitude` DOUBLE DEFAULT NULL,
    `seller_type` TEXT,
    `row_num` INT
)  ENGINE=INNODB DEFAULT CHARSET=UTF8MB4 COLLATE = UTF8MB4_0900_AI_CI;

SELECT 
    *
FROM
    cars_db_staging2;


INSERT INTO cars_db_staging2
SELECT *, ROW_NUMBER() OVER (
PARTITION BY price, brand, model, trim, kilometers, year, vehicle_age_years, regional_specs, doors, body_type, fuel_type, seating_capacity, transmission_type, engine_capacity_cc, horsepower, no_of_cylinders, exterior_color, interior_color, warranty, address, country, city, area_name, location_name, latitude, longitude, seller_type
) as row_num FROM cars_db_staging;


DELETE FROM cars_db_staging2 
WHERE
    row_num > 1;

-- STANDARDIZING DATA

SELECT 
    model, TRIM(model)
FROM
    cars_db_staging2;

UPDATE cars_db_staging2 
SET 
    model = TRIM(model);

SELECT DISTINCT
    engine_capacity_cc
FROM
    cars_db_staging2
ORDER BY 1;
SELECT 
    *
FROM
    cars_db_staging2
WHERE
    model LIKE '2%';-- here in mazda brand model is 2 and in bmw the model name is 2-Series; so standardization doesnt required

CREATE TABLE cars_sales_stage0 SELECT * FROM
    dubizzle_cars_dataset;
UPDATE dubizzle_cars_dataset 
SET 
    location_name = NULL
WHERE
    location_name = '';
SELECT 
    *
FROM
    dubizzle_cars_dataset;
SELECT 
    COUNT(*)
FROM
    dubizzle_cars_dataset
WHERE
    location_name IS NULL;-- 5075 null column


SELECT 
    COUNT(DISTINCT longitude)
FROM
    dubizzle_cars_dataset;-- 379

UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'AI Mamsha'
WHERE
    address = 'Dubai Marina, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'AI Sheif'
WHERE
    address = 'Al Safa, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'AI Quoz 22'
WHERE
    address = 'Al Quoz, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Unnamed Road'
WHERE
    address = 'Jumeirah Village Circle (JVC), Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = '7b Street'
WHERE
    address = 'Jumeirah, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Frond A and B'
WHERE
    address = 'Palm Jumeirah, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Business Bay'
WHERE
    address = 'Jumeirah Lake Towers (JLT), Dubai, UAE';

UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Cluster D'
WHERE
    address = 'Downtown Dubai, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Millennium Plaza Hotel'
WHERE
    address = 'Sheikh Zayed Road, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Hilton Dubai The Walk'
WHERE
    address = 'Jumeirah Beach Residence (JBR), Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Unnamed Road'
WHERE
    address = 'Al Jaddaf, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Zabeel'
WHERE
    address = 'Dubai Design District, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Springs 10'
WHERE
    address = 'The Springs, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = '19B Street'
WHERE
    address = 'Al Barsha, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Jogging Track'
WHERE
    address = 'Bur Dubai, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'AI Kheeran 1'
WHERE
    address = 'Ras Al Khor, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Madina AI Sajja'
WHERE
    address = 'Al Sajaa Industrial, Sharjah, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Unnamed Road'
WHERE
    address = 'Al Awir, Dubai, UAE';

UPDATE dubizzle_cars_dataset 
SET 
    location_name = '42c Street'
WHERE
    address = 'Al Mizhar, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'UAE Exchange'
WHERE
    address = 'Jebel Ali, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Hadaeq Sheikh Mohammed Bin Rashid'
WHERE
    address = 'Dubai Hills Estate, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'The Red Lounge'
WHERE
    address = 'Barsha Heights (Tecom), Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'AI Mamzar Beach Street'
WHERE
    address = 'Al Mamzar, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = '14a street'
WHERE
    address = 'Al Qusais, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Muwailih'
WHERE
    address = 'Sharjah University City, Sharjah, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'J-14 J Street'
WHERE
    address = 'International City, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'trucks parking'
WHERE
    address = 'Al Sajaa, Sharjah, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Rahat D3 Street'
WHERE
    address = 'Mudon, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Al Marbouh Street,'
WHERE
    address = 'Al Shamkha, Abu Dhabi, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Street 8'
WHERE
    address = 'Mussafah, Abu Dhabi, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Unnamed'
WHERE
    address = 'Muhaisnah, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Al Aamal Street'
WHERE
    address = 'Business Bay, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Dubai Square Parking'
WHERE
    address = 'Dubai Creek Harbour, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = '30 Al Hasad Street,'
WHERE
    address = 'Zayed City, Abu Dhabi, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = '27b Street'
WHERE
    address = 'Mirdif, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Nad Al Hamar Road'
WHERE
    address = 'Nad Al Hamar, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Corniche Street'
WHERE
    address = 'Al Majaz, Sharjah, UAE';

UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Dubai Festival City'
WHERE
    address = 'Dubai Festival City, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = '27a Street'
WHERE
    address = 'Muhaisnah, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Unn29 Hattan Avenue'
WHERE
    address = 'Arabian Ranches, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Al Thanyah 4'
WHERE
    address = 'Emirates Hills, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Tasjeel'
WHERE
    address = 'Discovery Gardens, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Al Riqqah Street'
WHERE
    address = 'Al Jazzat, Sharjah, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Bluewaters Wharf Link'
WHERE
    address = 'Bluewaters Island, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Unnamed'
WHERE
    address = 'Arabian Ranches 3, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Al Karamah'
WHERE
    address = 'Abu Dhabi, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'King Salman bin Abdulaziz Al Saud Street'
WHERE
    address = 'Dubai Media City, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Al Barsha South 3'
WHERE
    address = 'Arjan, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'MBR- Al Merkad'
WHERE
    address = 'Mohammed Bin Rashid City, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Fly Zone Trampoline Park'
WHERE
    address = 'Ras Al Khaimah, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Saadiyat Island Golf Course'
WHERE
    address = 'Saadiyat Island, Abu Dhabi, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = '949 Sheikha Fatima bint Mubarak Street'
WHERE
    address = 'Qasr El Bahr, Abu Dhabi, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = '459 Shakhbout Bin Sultan Street'
WHERE
    address = 'Al Mushrif, Abu Dhabi, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Unnamed'
WHERE
    address = 'Muhaisnah, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Al Hebiah 5'
WHERE
    address = 'Remraam, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Unnamed'
WHERE
    address = 'Al Khan, Sharjah, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Fuschia Street'
WHERE
    address = 'The Gardens, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Al Falah Street'
WHERE
    address = 'Al Falah Street, Abu Dhabi, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Dubai Safari Zoo Park'
WHERE
    address = 'Al Warqaa, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Unnamed'
WHERE
    address = 'Masdar City, Abu Dhabi, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Al Samar 1'
WHERE
    address = 'The Greens, Dubai, UAE';

SET autocommit = 0;

UPDATE dubizzle_cars_dataset 
SET 
    location_name = '27a Street'
WHERE
    address = 'Al Rashidiya, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Al Souk Al Kabeer'
WHERE
    address = 'Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Al Burj Street'
WHERE
    address = 'Al Muroor, Abu Dhabi, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Al Furjan'
WHERE
    address = 'Al Furjan, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Sharjah Emirate'
WHERE
    address = 'Sharjah, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = '19 Street'
WHERE
    address = 'Al Safa 1, Al Safa, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Al Falah'
WHERE
    address = 'Al Falah City, Abu Dhabi, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Trade Centre'
WHERE
    address = 'DIFC, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Hamdan Street'
WHERE
    address = 'Hamdan Street, Abu Dhabi, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Al Khan'
WHERE
    address = 'Al Qasba, Sharjah, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Ajman Emirate'
WHERE
    address = 'Ajman, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = '85 Al Quffal Street'
WHERE
    address = 'Al Bateen, Abu Dhabi, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = '19 Street'
WHERE
    address = 'Al Garhoud, Dubai, UAE';

UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'MSB Private School - Secondary Block'
WHERE
    address = 'Al Nahda (Dubai), Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Trump International Golf Club'
WHERE
    address = 'DAMAC Hills, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = '3 Street'
WHERE
    address = 'Dubailand, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Tulip Way'
WHERE
    address = 'Green Community, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Golf car path'
WHERE
    address = 'Dubai Sports City, Dubai, UAE';
UPDATE dubizzle_cars_dataset 
SET 
    location_name = area_name
WHERE
    location_name IS NULL;
commit;
SELECT 
    COLUMN_NAME
FROM
    INFORMATION_SCHEMA.COLUMNS
WHERE
    TABLE_NAME = 'dubizzle_cars_dataset';

-- set empty cell to null
UPDATE dubizzle_cars_dataset 
SET 
    price = NULLIF(price, ''),
    brand = NULLIF(brand, ''),
    model = NULLIF(model, ''),
    trim = NULLIF(trim, ''),
    kilometers = NULLIF(kilometers, ''),
    year = NULLIF(year, ''),
    vehicle_age_years = NULLIF(vehicle_age_years, ''),
    regional_specs = NULLIF(regional_specs, ''),
    doors = NULLIF(doors, ''),
    body_type = NULLIF(body_type, ''),
    fuel_type = NULLIF(fuel_type, ''),
    seating_capacity = NULLIF(seating_capacity, ''),
    transmission_type = NULLIF(transmission_type, ''),
    engine_capacity_cc = NULLIF(engine_capacity_cc, ''),
    horsepower = NULLIF(horsepower, ''),
    no_of_cylinders = NULLIF(no_of_cylinders, ''),
    exterior_color = NULLIF(exterior_color, ''),
    interior_color = NULLIF(interior_color, ''),
    warranty = NULLIF(warranty, ''),
    address = NULLIF(address, ''),
    country = NULLIF(country, ''),
    city = NULLIF(city, ''),
    area_name = NULLIF(area_name, ''),
    location_name = NULLIF(location_name, ''),
    latitude = NULLIF(latitude, ''),
    longitude = NULLIF(longitude, ''),
    seller_type = NULLIF(seller_type, '');

SELECT 
    *
FROM
    dubizzle_cars_dataset
WHERE
    price IS NULL;

UPDATE dubizzle_cars_dataset 
SET 
    engine_capacity_cc = '2000 - 2900cc'
WHERE
    brand = 'Alfa Romeo'
        AND model = 'Stelvio';
UPDATE dubizzle_cars_dataset 
SET 
    engine_capacity_cc = '3000 - 5000cc'
WHERE
    brand = 'Land Rover'
        AND model = 'Range Rover';
UPDATE dubizzle_cars_dataset 
SET 
    engine_capacity_cc = '3000 - 4000cc'
WHERE
    brand = 'Porsche'
        AND model = 'Carrera / 911';
UPDATE dubizzle_cars_dataset 
SET 
    engine_capacity_cc = '6000+ cc'
WHERE
    brand = 'Rolls-Royce'
        AND model = 'Ghost';
UPDATE dubizzle_cars_dataset 
SET 
    engine_capacity_cc = '6000cc'
WHERE
    brand = 'Rolls-Royce'
        AND model = 'Cullinan';
UPDATE dubizzle_cars_dataset 
SET 
    engine_capacity_cc = '1600 - 4000cc'
WHERE
    brand = 'Mercedes-Benz'
        AND model = 'C-Class';
UPDATE dubizzle_cars_dataset 
SET 
    engine_capacity_cc = '2000 - 4000cc'
WHERE
    brand = 'Porsche' AND model = 'Boxster';
UPDATE dubizzle_cars_dataset 
SET 
    engine_capacity_cc = '2000 - 4000cc'
WHERE
    brand = 'Mercedes-Benz'
        AND model = 'E-Class Coupe';
UPDATE dubizzle_cars_dataset 
SET 
    engine_capacity_cc = '4000 - 5600cc'
WHERE
    brand = 'Nissan' AND model = 'Patrol';
UPDATE dubizzle_cars_dataset 
SET 
    engine_capacity_cc = '2700 - 7300cc'
WHERE
    brand = 'Ford'
        AND model = 'F-Series Pickup';
UPDATE dubizzle_cars_dataset 
SET 
    engine_capacity_cc = '3000 - 4800cc'
WHERE
    brand = 'Porsche' AND model = 'Cayenne';
UPDATE dubizzle_cars_dataset 
SET 
    engine_capacity_cc = '1000 - 2000cc'
WHERE
    brand = 'MINI' AND model = 'Cooper';
UPDATE dubizzle_cars_dataset 
SET 
    engine_capacity_cc = '1000 - 1500cc'
WHERE
    brand = 'MG' AND model = 'ZS';
UPDATE dubizzle_cars_dataset 
SET 
    engine_capacity_cc = '4000 - 6000cc'
WHERE
    brand = 'Bentley' AND model = 'Bentayga';
UPDATE dubizzle_cars_dataset 
SET 
    engine_capacity_cc = '2800 - 4000cc'
WHERE
    brand = 'Mercedes-Benz'
        AND model = 'G-Class';
UPDATE dubizzle_cars_dataset 
SET 
    engine_capacity_cc = '3800cc'
WHERE
    brand = 'Ferrari' AND model = 'Roma';
UPDATE dubizzle_cars_dataset 
SET 
    engine_capacity_cc = '4000cc'
WHERE
    brand = 'Lamborghini' AND model = 'Urus';

SELECT 
    *
FROM
    dubizzle_cars_dataset
WHERE
    brand = 'Alfa Romeo'
        AND model = 'Stelvio';
UPDATE dubizzle_cars_dataset 
SET 
    engine_capacity_cc = CASE
        WHEN
            brand = 'Land Rover'
                AND model = 'Range Rover Sport'
        THEN
            '2000-5000 cc'
        WHEN brand = 'Jetour' AND model = 'Dashing' THEN '1500-2000 cc'
        WHEN brand = 'Ford' AND model = 'Mustang' THEN '2000-5000 cc'
        WHEN
            brand = 'Lamborghini'
                AND model = 'Huracan'
        THEN
            '5200-6400 cc'
        WHEN
            brand = 'Bentley'
                AND model = 'Continental GTC'
        THEN
            '4000-6000 cc'
        WHEN
            brand = 'Land Rover'
                AND model = 'Defender'
        THEN
            '2000-3000 cc'
        WHEN
            brand = 'Rolls-Royce'
                AND model = 'Spectre'
        THEN
            'Unknown'
    END
WHERE
    brand IN ('Land Rover' , 'Jetour',
        'Ford',
        'Lamborghini',
        'Bentley',
        'Rolls-Royce')
        AND model IN ('Range Rover Sport' , 'Dashing',
        'Mustang',
        'Huracan',
        'Continental GTC',
        'Defender',
        'Spectre');

UPDATE dubizzle_cars_dataset 
SET 
    engine_capacity_cc = CASE
        WHEN brand = 'Audi' AND model = 'S3/RS3' THEN '1984 cc'
        WHEN brand = 'Nissan' AND model = 'Urvan' THEN '2488 cc'
        WHEN
            brand = 'SsangYong'
                AND model = 'Korando'
        THEN
            '1597-1998 cc'
        WHEN brand = 'Ferrari' AND model = '296 GTS' THEN '2992 cc'
        WHEN
            brand = 'Mercedes-Benz'
                AND model = 'GLE Coupe'
        THEN
            '1991-3982 cc'
        WHEN
            brand = 'Jeep'
                AND model = 'Wrangler Unlimited'
        THEN
            '1998-3604 cc'
        WHEN brand = 'BMW' AND model = 'X5' THEN '1998-4395 cc'
        WHEN
            brand = 'Rolls-Royce'
                AND model = 'Wraith'
        THEN
            '6592 cc'
        WHEN
            brand = 'Land Rover'
                AND model = 'Range Rover Velar'
        THEN
            '1997-2996 cc'
        WHEN brand = 'Dodge' AND model = 'Ram' THEN '3604-5654 cc'
        WHEN brand = 'Audi' AND model = 'Q7' THEN '1984-3956 cc'
        WHEN brand = 'Jaguar' AND model = 'F-Pace' THEN '1997-2996 cc'
        WHEN brand = 'BMW' AND model = '7-Series' THEN '1998-6592 cc'
        WHEN
            brand = 'Mercedes-Benz'
                AND model = 'GLE-Class'
        THEN
            '2143-3982 cc'
        WHEN
            brand = 'Mercedes-Benz'
                AND model = 'CLS-Class'
        THEN
            '1950-3982 cc'
        WHEN brand = 'Audi' AND model = 'A8' THEN '2967-3996 cc'
        WHEN
            brand = 'Mercedes-Benz'
                AND model = 'E-Class'
        THEN
            '1950-3982 cc'
        WHEN brand = 'Porsche' AND model = 'Taycan' THEN '2800-3510 cc'
        WHEN brand = 'Ford' AND model = 'Ranger' THEN '2198-3198 cc'
        WHEN
            brand = 'Ferrari'
                AND model = 'SF90 Stradale'
        THEN
            '3990 cc'
        WHEN brand = 'Ferrari' AND model = '296 GTB' THEN '2992 cc'
        WHEN
            brand = 'Mercedes-Benz'
                AND model = 'SL-Class'
        THEN
            '2996-3982 cc'
        WHEN
            brand = 'Mercedes-Benz'
                AND model = 'S-Class'
        THEN
            '2925-5980 cc'
        WHEN
            brand = 'Volkswagen'
                AND model = 'Tiguan'
        THEN
            '1395-1984 cc'
        WHEN
            brand = 'Mitsubishi'
                AND model = 'Pajero'
        THEN
            '2442-3200 cc'
        WHEN
            brand = 'Mercedes-Benz'
                AND model = 'GLA'
        THEN
            '1332-1991 cc'
        WHEN
            brand = 'Rolls-Royce'
                AND model = 'Phantom'
        THEN
            '6749 cc'
        WHEN
            brand = 'Mercedes-Benz'
                AND model = 'GLS-Class'
        THEN
            '2925-3982 cc'
        WHEN
            brand = 'Land Rover'
                AND model = 'Range Rover Evoque'
        THEN
            '1997-1999 cc'
    END
WHERE
    brand IN ('Audi' , 'Nissan',
        'SsangYong',
        'Ferrari',
        'Mercedes-Benz',
        'Jeep',
        'BMW',
        'Rolls-Royce',
        'Land Rover',
        'Dodge',
        'Jaguar',
        'Porsche',
        'Ford',
        'Volkswagen',
        'Mitsubishi')
        AND model IN ('S3/RS3' , 'Urvan',
        'Korando',
        '296 GTS',
        'GLE Coupe',
        'Wrangler Unlimited',
        'X5',
        'Wraith',
        'Range Rover Velar',
        'Ram',
        'Q7',
        'F-Pace',
        '7-Series',
        'GLE-Class',
        'CLS-Class',
        'A8',
        'E-Class',
        'Taycan',
        'Ranger',
        'SF90 Stradale',
        '296 GTB',
        'SL-Class',
        'S-Class',
        'Tiguan',
        'Pajero',
        'GLA',
        'Phantom',
        'GLS-Class',
        'Range Rover Evoque');
    
    
UPDATE dubizzle_cars_dataset 
SET 
    engine_capacity_cc = CASE
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mercedes-AMG'
                AND model = 'GT'
        THEN
            '3982 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Toyota'
                AND model = 'Fortuner'
        THEN
            '2393-2755 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Audi'
                AND model = 'RS Q8'
        THEN
            '3996 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Bentley'
                AND model = 'Continental'
        THEN
            '3993-5998 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Lexus'
                AND model = 'RX-Series'
        THEN
            '2494-3456 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Toyota'
                AND model = 'Rush'
        THEN
            '1496 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Toyota'
                AND model = 'Rav 4'
        THEN
            '1987-2487 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Honda'
                AND model = 'City'
        THEN
            '1498 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Tesla'
                AND model = 'Model Y'
        THEN
            '0 cc (electric)'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Jetour'
                AND model = 'X70'
        THEN
            '1499 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'BMW'
                AND model = 'X6'
        THEN
            '2993-4395 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Lexus'
                AND model = 'TX'
        THEN
            '2494-3456 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'GMC'
                AND model = 'Hummer'
        THEN
            '0 cc (electric)'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Ferrari'
                AND model = '812 GTS'
        THEN
            '6496 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Ferrari'
                AND model = 'F8 Tributo'
        THEN
            '3902 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Hyundai'
                AND model = 'Palisade'
        THEN
            '2199-3470 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Toyota'
                AND model = 'Prado'
        THEN
            '2693-4461 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Jaguar'
                AND model = 'E-Pace'
        THEN
            '1998-1999 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'BMW'
                AND model = '4-Series'
        THEN
            '1998-2993 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mercedes-Benz'
                AND model = 'A-Class'
        THEN
            '1332-1991 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'BMW'
                AND model = '5-Series'
        THEN
            '1998-4395 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Porsche'
                AND model = 'Macan'
        THEN
            '1984-2995 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'GMC'
                AND model = 'Yukon'
        THEN
            '5300-6200 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mercedes-Benz'
                AND model = 'GT'
        THEN
            '3982 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Bentley'
                AND model = 'Continental GT'
        THEN
            '3993-5998 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Volkswagen'
                AND model = 'Teramont'
        THEN
            '1984-2995 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Tesla'
                AND model = 'Model 3'
        THEN
            '0 cc (electric)'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Lexus'
                AND model = 'ES-Series'
        THEN
            '2494-3456 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Toyota'
                AND model = 'Sequoia'
        THEN
            '5663 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Kia'
                AND model = 'Cadenza'
        THEN
            '2359-2999 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Ferrari'
                AND model = '488 Pista Spider'
        THEN
            '3902 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Infiniti'
                AND model = 'QX80'
        THEN
            '5552 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Infiniti'
                AND model = 'QX55'
        THEN
            '1997 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Hyundai'
                AND model = 'Accent'
        THEN
            '1368-1498 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Kia'
                AND model = 'Sportage'
        THEN
            '1591-2359 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Nissan'
                AND model = 'Pathfinder'
        THEN
            '3498 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Cadillac'
                AND model = 'Escalade'
        THEN
            '5300-6200 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'BMW'
                AND model = 'X4'
        THEN
            '1998-2993 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Jeep'
                AND model = 'Wrangler'
        THEN
            '1998-3604 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Hyundai'
                AND model = 'H1'
        THEN
            '2359-2497 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mitsubishi'
                AND model = 'Xpander'
        THEN
            '1499 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Subaru'
                AND model = 'WRX'
        THEN
            '2457 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Chevrolet'
                AND model = 'Captiva'
        THEN
            '1998 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Jeep'
                AND model = 'Cherokee'
        THEN
            '1998-3239 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Kia'
                AND model = 'Sorento'
        THEN
            '2199-3470 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mazda'
                AND model = 'CX-3'
        THEN
            '1496-1998 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Changan'
                AND model = 'CS85'
        THEN
            '1499 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Nissan'
                AND model = 'Altima'
        THEN
            '1997-2488 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Kia'
                AND model = 'Carnival'
        THEN
            '2199-3470 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Jeep'
                AND model = 'Grand Cherokee'
        THEN
            '3604-6417 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Ferrari'
                AND model = 'F8 Spider'
        THEN
            '3902 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mercedes-Benz'
                AND model = 'GL-Class'
        THEN
            '2987-5461 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Honda'
                AND model = 'Odyssey'
        THEN
            '2356-3471 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Nissan'
                AND model = 'X-Trail'
        THEN
            '1997-2488 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Audi'
                AND model = 'Q5'
        THEN
            '1984-2995 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mercedes-Benz'
                AND model = 'EQS'
        THEN
            '108 kWh (electric)'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Peugeot'
                AND model = '3008'
        THEN
            '1199-1598 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Toyota'
                AND model = 'Land Cruiser'
        THEN
            '4461 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Volkswagen'
                AND model = 'CC'
        THEN
            '1395-1984 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Audi'
                AND model = 'S8'
        THEN
            '3996 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Nissan'
                AND model = 'Sunny'
        THEN
            '1198-1598 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Ford'
                AND model = 'Pickup'
        THEN
            '2261-3198 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Nissan'
                AND model = 'Versa'
        THEN
            '1498 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Audi'
                AND model = 'Q8'
        THEN
            '1984-2995 cc'
    END
WHERE
    engine_capacity_cc IS NULL
        AND brand IN ('Mercedes-AMG' , 'Toyota',
        'Audi',
        'Bentley',
        'Lexus',
        'Honda',
        'Tesla',
        'Jetour',
        'BMW',
        'GMC',
        'Ferrari',
        'Hyundai',
        'Jaguar',
        'Porsche',
        'Ford',
        'Volkswagen',
        'Peugeot',
        'Subaru',
        'Chevrolet',
        'Changan',
        'Mitsubishi',
        'Cadillac',
        'Kia',
        'Infiniti',
        'Mazda',
        'Nissan')
        AND model IN ('GT' , 'Fortuner',
        'RS Q8',
        'Continental',
        'RX-Series',
        'Rush',
        'Rav 4',
        'City',
        'Model Y',
        'X70',
        'X6',
        'TX',
        'Hummer',
        '812 GTS',
        'F8 Tributo',
        'Palisade',
        'Prado',
        'E-Pace',
        '4-Series',
        'A-Class',
        '5-Series',
        'Macan',
        'Yukon',
        'GT',
        'Continental GT',
        'Teramont',
        'Model 3',
        'ES-Series',
        'Sequoia',
        'Cadenza',
        '488 Pista Spider',
        'QX80',
        'QX55',
        'Accent',
        'Sportage',
        'Pathfinder',
        'Escalade',
        'X4',
        'Wrangler',
        'H1',
        'Xpander',
        'WRX',
        'Captiva',
        'Cherokee',
        'Sorento',
        'CX-3',
        'CS85',
        'Altima',
        'Carnival',
        'Grand Cherokee',
        'F8 Spider',
        'GL-Class',
        'Odyssey',
        'X-Trail',
        'Q5',
        'EQS',
        '3008',
        'Land Cruiser',
        'CC',
        'S8',
        'Sunny',
        'Pickup',
        'Versa',
        'Q8');


UPDATE dubizzle_cars_dataset 
SET 
    engine_capacity_cc = CASE
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mercedes-Benz'
                AND model = 'A-Class'
        THEN
            '1332-1991 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mercedes-Benz'
                AND model = 'GT'
        THEN
            '3982 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Ford'
                AND model = 'GT'
        THEN
            '3497 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Jeep'
                AND model = 'Wrangler'
        THEN
            '1998-3604 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Jeep'
                AND model = 'Cherokee'
        THEN
            '1998-3239 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Jeep'
                AND model = 'Grand Cherokee'
        THEN
            '3604-6417 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mercedes-Benz'
                AND model = 'GL-Class'
        THEN
            '2987-5461 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mercedes-Benz'
                AND model = 'EQS'
        THEN
            '108 kWh (electric)'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Nissan'
                AND model = 'Pickup'
        THEN
            '2488-3198 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mercedes-Benz'
                AND model = 'CLA'
        THEN
            '1332-1991 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Nissan'
                AND model = 'Navara'
        THEN
            '2488-2298 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Jaguar'
                AND model = 'XK'
        THEN
            '2995-5000 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Volvo'
                AND model = 'XC40'
        THEN
            '1969 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mitsubishi'
                AND model = 'L200'
        THEN
            '2442-2477 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Suzuki'
                AND model = 'Ertiga'
        THEN
            '1462 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Kia'
                AND model = 'Picanto'
        THEN
            '1248 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Great Wall'
                AND model = 'Wingle 5'
        THEN
            '1996 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Toyota'
                AND model = 'Corolla'
        THEN
            '1598-1987 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Kia'
                AND model = 'K5'
        THEN
            '1591-1999 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Toyota'
                AND model = 'Camry'
        THEN
            '1987-2494 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Hyundai'
                AND model = 'Santa Fe'
        THEN
            '1999-2359 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Hyundai'
                AND model = 'Venue'
        THEN
            '1197 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Suzuki'
                AND model = 'Baleno'
        THEN
            '998-1373 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Fiat'
                AND model = 'Fiat-500'
        THEN
            '875-1368 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mercedes-Benz'
                AND model = 'AMG'
        THEN
            '3982 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Rolls-Royce'
                AND model = 'Dawn'
        THEN
            '6592 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Ferrari'
                AND model = '488 Spider'
        THEN
            '3902 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'McLaren'
                AND model = '765LT'
        THEN
            '3994 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Bentley'
                AND model = 'Continental Flying Spur'
        THEN
            '3993-5998 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Ferrari'
                AND model = '812 Superfast'
        THEN
            '6496 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'BMW'
                AND model = 'M3'
        THEN
            '2979-2993 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Nissan'
                AND model = 'Armada'
        THEN
            '5552 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'BMW'
                AND model = 'X1'
        THEN
            '1499-1998 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mercedes-Benz'
                AND model = 'GLC'
        THEN
            '1950-2999 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'BMW'
                AND model = '3-Series'
        THEN
            '1499-1998 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Infiniti'
                AND model = 'QX50'
        THEN
            '1997 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mitsubishi'
                AND model = 'Attrage'
        THEN
            '1193 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Audi'
                AND model = 'TT'
        THEN
            '1984 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Jeep'
                AND model = 'Compass'
        THEN
            '1368-1998 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'JAC'
                AND model = 'J7'
        THEN
            '1499 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Suzuki'
                AND model = 'Vitara'
        THEN
            '1373-1586 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mitsubishi'
                AND model = 'Eclipse Cross'
        THEN
            '1499-1998 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Jeep'
                AND model = 'Renegade'
        THEN
            '1368-1998 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Audi'
                AND model = 'A3'
        THEN
            '999-1984 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Nissan'
                AND model = '370z'
        THEN
            '3696 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Volkswagen'
                AND model = 'Touareg'
        THEN
            '2967-3996 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Toyota'
                AND model = 'Cressida'
        THEN
            '2788 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Peugeot'
                AND model = '208'
        THEN
            '1199-1598 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'MG'
                AND model = 'RX8'
        THEN
            '1998 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Renault'
                AND model = 'Koleos'
        THEN
            '1598-1998 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Lexus'
                AND model = 'CT-Series'
        THEN
            '1798 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Haval'
                AND model = 'H2'
        THEN
            '1497 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Chery'
                AND model = 'Tiggo 5'
        THEN
            '1498-1598 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Nissan'
                AND model = 'Sentra'
        THEN
            '1498-1598 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Ferrari'
                AND model = 'Purosangue'
        THEN
            '6496 cc'
    END
WHERE
    engine_capacity_cc IS NULL
        AND brand IN ('Mercedes-Benz' , 'Ford',
        'Jeep',
        'Nissan',
        'Jaguar',
        'Volvo',
        'Toyota',
        'Mitsubishi',
        'Suzuki',
        'Kia',
        'Great Wall',
        'Fiat',
        'Rolls-Royce',
        'Ferrari',
        'McLaren',
        'Bentley',
        'BMW',
        'Infiniti',
        'Audi',
        'Maserati',
        'Lexus',
        'Maserati',
        'MG',
        'Renault',
        'Haval',
        'Chery',
        'Peugeot',
        'Subaru',
        'Hyundai',
        'SsangYong',
        'Tesla',
        'GAC',
        'GAC Gonow',
        'Geely',
        'Honda',
        'JAC',
        'Land Rover',
        'Lotus',
        'MG',
        'MINI',
        'Proton',
        'SEAT',
        'Skoda',
        'Toyota',
        'Volkswagen',
        'Volvo',
        'Zotye')
        AND model IN ('A-Class' , 'GT',
        'GT',
        'Wrangler',
        'Cherokee',
        'Grand Cherokee',
        'GL-Class',
        'EQS',
        'Pickup',
        'CLA',
        'Navara',
        'XK',
        'XC40',
        'Other',
        'L200',
        'Ertiga',
        'Picanto',
        'Wingle 5',
        'Corolla',
        'K5',
        'Other',
        'Camry',
        'Santa Fe',
        'Venue',
        'Baleno',
        'Fiat-500',
        'AMG',
        'Dawn',
        '488 Spider',
        'Purosangue',
        '765LT',
        'Continental Flying Spur',
        '812 Superfast',
        'M3',
        'Armada',
        'X1',
        'GLC',
        '3-Series',
        'QX50',
        'Attrage',
        'TT',
        'Compass',
        'J7',
        'Vitara',
        'Eclipse Cross',
        'Renegade',
        'A3',
        '370z',
        'Touareg',
        'Cressida',
        '208',
        'RX8',
        'Koleos',
        'CT-Series',
        'H2',
        'Tiggo 5',
        'Sentra');

SELECT 
    *
FROM
    cars_sales_stage0
WHERE
    brand = 'GAC' AND model = 'GS8';
UPDATE dubizzle_cars_dataset 
SET 
    engine_capacity_cc = CASE
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mitsubishi'
                AND model = 'EclipseCross'
        THEN
            '1499-1998 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Renault'
                AND model = 'Duster'
        THEN
            '1598-1998 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Lexus'
                AND model = 'GX-Series'
        THEN
            '3956-4608 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Volkswagen'
                AND model = 'Passat'
        THEN
            '1395-1984 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'GAC'
                AND model = 'GS4'
        THEN
            '1497 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Renault'
                AND model = 'Captur'
        THEN
            '898-1490 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'MG'
                AND model = 'RX5'
        THEN
            '1497-1798 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Ford'
                AND model = 'Fusion'
        THEN
            '1499-1999 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Nissan'
                AND model = 'Kicks'
        THEN
            '999-1598 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Ford'
                AND model = 'Ecosport'
        THEN
            '999-1499 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Land Rover'
                AND model = 'LR4'
        THEN
            '2995 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Honda'
                AND model = 'Accord'
        THEN
            '1498-1996 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'MG'
                AND model = 'MG6'
        THEN
            '1498 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Toyota'
                AND model = 'Avalon'
        THEN
            '2672-3456 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mitsubishi'
                AND model = 'ASX'
        THEN
            '1499-1998 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Volkswagen'
                AND model = 'Jetta'
        THEN
            '999-1984 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Hyundai'
                AND model = 'Veloster'
        THEN
            '1591 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Peugeot'
                AND model = '508'
        THEN
            '1199-1598 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'GAC'
                AND model = 'GS3'
        THEN
            '1498 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Alfa Romeo'
                AND model = 'GIULIETTA'
        THEN
            '1368-1956 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Kia'
                AND model = 'Seltos'
        THEN
            '1353-1591 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Suzuki'
                AND model = 'Jimny'
        THEN
            '1462 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Ford'
                AND model = 'Fiesta'
        THEN
            '999-1499 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Volvo'
                AND model = 'XC70'
        THEN
            '1969-2400 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Hyundai'
                AND model = 'Creta'
        THEN
            '1368-1497 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Opel'
                AND model = 'Mokka'
        THEN
            '1199-1598 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mitsubishi'
                AND model = 'Lancer'
        THEN
            '1499-1998 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Nissan'
                AND model = 'Tiida'
        THEN
            '1198-1498 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Peugeot'
                AND model = '308'
        THEN
            '1199-1598 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'BAIC'
                AND model = 'D20'
        THEN
            '1342-1499 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'BMW'
                AND model = 'X3'
        THEN
            '1998-2993 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Ford'
                AND model = 'Escape'
        THEN
            '1499-1999 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Cadillac'
                AND model = 'XTS'
        THEN
            '3649 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Ford'
                AND model = 'Crown Victoria'
        THEN
            '4601 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Ford'
                AND model = 'Explorer'
        THEN
            '1999-3496 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Toyota'
                AND model = 'Urban Cruiser'
        THEN
            '1462 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Tesla'
                AND model = 'Model S'
        THEN
            '0 cc (electric)'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'BMW'
                AND model = 'X2'
        THEN
            '1499-1998 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'BMW'
                AND model = '2-Series'
        THEN
            '1499-1998 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Infiniti'
                AND model = 'FX50'
        THEN
            '5026 cc'
    END
WHERE
    engine_capacity_cc IS NULL
        AND brand IN ('Mitsubishi' , 'Renault',
        'Lexus',
        'Volkswagen',
        'GAC',
        'MG',
        'Ford',
        'Nissan',
        'Land Rover',
        'Honda',
        'Toyota',
        'Alfa Romeo',
        'Kia',
        'Suzuki',
        'Volvo',
        'Hyundai',
        'Opel',
        'Peugeot',
        'BAIC',
        'Cadillac',
        'BMW',
        'Tesla',
        'Infiniti')
        AND model IN ('EclipseCross' , 'Duster',
        'GX-Series',
        'Passat',
        'GS4',
        'Captur',
        'RX5',
        'Fusion',
        'Kicks',
        'Ecosport',
        'LR4',
        'Accord',
        'MG6',
        'Avalon',
        'ASX',
        'Jetta',
        'Veloster',
        '508',
        'GS3',
        'GIULIETTA',
        'Seltos',
        'Jimny',
        'Fiesta',
        'XC70',
        'Creta',
        'Mokka',
        'Lancer',
        'Tiida',
        '308',
        'D20',
        'X3',
        'Escape',
        'XTS',
        'Crown Victoria',
        'Explorer',
        'Urban Cruiser',
        'Model S',
        'X2',
        '2-Series',
        'FX50');

UPDATE dubizzle_cars_dataset 
SET 
    engine_capacity_cc = CASE
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Jaguar'
                AND model = 'F-Type'
        THEN
            '1997-5000 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Lexus'
                AND model = 'LX570'
        THEN
            '5663 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Toyota'
                AND model = 'Highlander'
        THEN
            '2494-3456 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Lexus'
                AND model = 'NX 350'
        THEN
            '1987-2499 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'TANK'
                AND model = '500'
        THEN
            '4998 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Toyota'
                AND model = 'Granvia'
        THEN
            '2759 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'TANK'
                AND model = '300'
        THEN
            '2999 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Toyota'
                AND model = 'Hilux'
        THEN
            '2393-2755 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'BMW'
                AND model = 'i7'
        THEN
            '2500-5000 cc (electric)'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'BMW'
                AND model = '6-Series'
        THEN
            '1998-4395 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Ford'
                AND model = 'Expedition'
        THEN
            '3496 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Audi'
                AND model = 'A6'
        THEN
            '1984-3996 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Audi'
                AND model = 'e-tron'
        THEN
            '0 cc (electric)'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Lincoln'
                AND model = 'MKZ'
        THEN
            '1999-3496 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Volkswagen'
                AND model = 'Arteon'
        THEN
            '1984 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Hyundai'
                AND model = 'Tucson'
        THEN
            '1493-1999 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Volkswagen'
                AND model = 'Golf R'
        THEN
            '1984 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Volkswagen'
                AND model = 'Beetle'
        THEN
            '1395-1984 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mercedes-Benz'
                AND model = 'V-Class'
        THEN
            '2143-2987 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'BMW'
                AND model = 'M8'
        THEN
            '4395 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mercedes-Benz'
                AND model = 'M-Class'
        THEN
            '2143-5461 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Lexus'
                AND model = 'IS300'
        THEN
            '1998-2499 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Infiniti'
                AND model = 'QX70'
        THEN
            '3696 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Maserati'
                AND model = 'GranTurismo'
        THEN
            '4244-4691 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mercedes-Benz'
                AND model = 'GLK-Class'
        THEN
            '2143-3498 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Audi'
                AND model = 'A5'
        THEN
            '1395-2995 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Abarth'
                AND model = '695'
        THEN
            '1368 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Volkswagen'
                AND model = 'ID.6'
        THEN
            '0 cc (electric)'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Maserati'
                AND model = 'Ghibli'
        THEN
            '1998-2979 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Honda'
                AND model = 'CR-V'
        THEN
            '1498-1997 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mercedes-Benz'
                AND model = 'Sprinter'
        THEN
            '2143-2987 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Dodge'
                AND model = 'Challenger'
        THEN
            '3604-6417 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Ferrari'
                AND model = '488'
        THEN
            '3902 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Ferrari'
                AND model = '488 Pista'
        THEN
            '3902 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mercedes-Benz'
                AND model = 'SLS'
        THEN
            '6208 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Nissan'
                AND model = 'Xterra'
        THEN
            '3954-4663 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Toyota'
                AND model = 'FJ Cruiser'
        THEN
            '3956 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'MG'
                AND model = 'MG5'
        THEN
            '1498 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Toyota'
                AND model = 'Avanza'
        THEN
            '1496 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Aston Martin'
                AND model = 'Vantage'
        THEN
            '3982 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Ferrari'
                AND model = 'Portofino M'
        THEN
            '3855 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mercedes-AMG'
                AND model = 'GT S'
        THEN
            '3982 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Chevrolet'
                AND model = 'Corvette'
        THEN
            '6162 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Aston Martin'
                AND model = 'DBS'
        THEN
            '5204 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mercedes-AMG'
                AND model = 'GT R'
        THEN
            '3982 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Aston Martin'
                AND model = 'DB11'
        THEN
            '3982 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Ford'
                AND model = 'Edge'
        THEN
            '1999-3498 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'BMW'
                AND model = 'M4'
        THEN
            '2979-2993 cc'
    END
WHERE
    engine_capacity_cc IS NULL
        AND brand IN ('Jaguar' , 'Lexus',
        'Toyota',
        'TANK',
        'BMW',
        'Ford',
        'Audi',
        'Lincoln',
        'Volkswagen',
        'Hyundai',
        'Mercedes-Benz',
        'Infiniti',
        'Maserati',
        'Honda',
        'Dodge',
        'Abarth',
        'Nissan',
        'MG',
        'Aston Martin',
        'Ferrari',
        'Chevrolet')
        AND model IN ('F-Type' , 'LX570',
        'Highlander',
        'NX 350',
        '500',
        'Granvia',
        '300',
        'Hilux',
        'i7',
        '6-Series',
        'Expedition',
        'A6',
        'e-tron',
        'MKZ',
        'Arteon',
        'Tucson',
        'Golf R',
        'Beetle',
        'V-Class',
        'M8',
        'M-Class',
        'IS300',
        'QX70',
        'GranTurismo',
        'GLK-Class',
        'A5',
        '695',
        'ID.6',
        'Ghibli',
        'CR-V',
        'Sprinter',
        'Challenger',
        '488',
        '488 Pista',
        'SLS',
        'Xterra',
        'FJ Cruiser',
        'MG5',
        'Avanza',
        'Vantage',
        'Portofino M',
        'GT S',
        'Corvette',
        'DBS',
        'GT R',
        'DB11',
        'Edge',
        'M4');

UPDATE dubizzle_cars_dataset 
SET 
    engine_capacity_cc = CASE
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mercedes-AMG'
                AND model IN ('GT S' , 'GT R', 'GT Black Series')
        THEN
            '3982 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Chrysler'
                AND model = '300'
        THEN
            '3604-6417 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mercedes-Maybach'
                AND model = 'GLS-Class'
        THEN
            '3982-5980 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'GMC'
                AND model = 'Sierra'
        THEN
            '2776-6200 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Porsche'
                AND model = 'Panamera'
        THEN
            '2995-3996 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Lexus'
                AND model = 'IS-C'
        THEN
            '2499-3500 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mercedes-Benz'
                AND model = 'GLB'
        THEN
            '1332-1991 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Lexus'
                AND model = 'LX-Series'
        THEN
            '5663 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Volkswagen'
                AND model = 'ID.4'
        THEN
            '0 cc (electric)'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Ford'
                AND model = 'Focus'
        THEN
            '999-1499 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Volkswagen'
                AND model = 'GTI'
        THEN
            '1984 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Nissan'
                AND model = 'Juke'
        THEN
            '999-1618 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Toyota'
                AND model = 'Pickup'
        THEN
            '2693-3956 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'BMW'
                AND model = '8-Series'
        THEN
            '2998-4395 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Lamborghini'
                AND model = 'Aventador'
        THEN
            '6498 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mazda'
                AND model = 'CX-9'
        THEN
            '2488 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Suzuki'
                AND model = 'Swift'
        THEN
            '1197-1373 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Toyota'
                AND model = 'IQ'
        THEN
            '996 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Hyundai'
                AND model = 'Kona'
        THEN
            '998-1999 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Infiniti'
                AND model = 'Q50'
        THEN
            '1991-2997 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Dodge'
                AND model = 'Durango'
        THEN
            '3604-6417 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Kia'
                AND model = 'Bongo'
        THEN
            '2497 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Audi'
                AND model IN ('S6' , 'RS6')
        THEN
            '2995-3996 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Maserati'
                AND model = 'MC20'
        THEN
            '2997 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Nissan'
                AND model = 'Micra'
        THEN
            '898-1198 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Lincoln'
                AND model = 'Aviator'
        THEN
            '3496 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'JAC'
                AND model = 'JS4'
        THEN
            '1499 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Hyundai'
                AND model = 'Azera'
        THEN
            '2497-3470 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'BMW'
                AND model = 'i3'
        THEN
            '0 cc (electric)'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Lincoln'
                AND model = 'MKX'
        THEN
            '2694 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Chevrolet'
                AND model = 'Suburban'
        THEN
            '5328-6600 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'BMW'
                AND model = 'Z3'
        THEN
            '1796-3201 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mercedes-Benz'
                AND model = 'CL-Class'
        THEN
            '4663-5980 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Ferrari'
                AND model IN ('Portofino' , 'Portofino M')
        THEN
            '3855 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Maserati'
                AND model = 'Levante'
        THEN
            '2987-3799 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Fisker'
                AND model = 'Karma'
        THEN
            '1998 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Honda'
                AND model = 'Civic'
        THEN
            '1498-1996 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mercedes-Benz'
                AND model = 'C-Class Coupe'
        THEN
            '1498-2999 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Volkswagen'
                AND model = 'Golf'
        THEN
            '999-1984 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Cadillac'
                AND model = 'XT5'
        THEN
            '1998-3649 cc'
    END
WHERE
    engine_capacity_cc IS NULL
        AND brand IN ('Mercedes-AMG' , 'Chrysler',
        'Mercedes-Maybach',
        'GMC',
        'Porsche',
        'Lexus',
        'Mercedes-Benz',
        'Volkswagen',
        'Ford',
        'Nissan',
        'Toyota',
        'BMW',
        'Lamborghini',
        'Mazda',
        'Suzuki',
        'Hyundai',
        'Infiniti',
        'Dodge',
        'Kia',
        'Audi',
        'Maserati',
        'Lincoln',
        'JAC',
        'Chevrolet',
        'Ferrari',
        'Fisker',
        'Honda',
        'Cadillac')
        AND model IN ('GT S' , 'GT R',
        'GT Black Series',
        '300',
        'GLS-Class',
        'Sierra',
        'Panamera',
        'IS-C',
        'GLB',
        'LX-Series',
        'ID.4',
        'Focus',
        'GTI',
        'Juke',
        'Pickup',
        '8-Series',
        'Aventador',
        'CX-9',
        'Swift',
        'IQ',
        'Kona',
        'Q50',
        'Durango',
        'Bongo',
        'S6',
        'RS6',
        'MC20',
        'Micra',
        'Aviator',
        'JS4',
        'Azera',
        'i3',
        'MKX',
        'Suburban',
        'Z3',
        'CL-Class',
        'Portofino',
        'Portofino M',
        'Levante',
        'Karma',
        'Civic',
        'C-Class Coupe',
        'Golf',
        'XT5');

UPDATE dubizzle_cars_dataset 
SET 
    engine_capacity_cc = CASE
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Audi'
                AND model = 'S6/RS6'
        THEN
            '2995-3996 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Lincoln'
                AND model = 'Avaiator'
        THEN
            '3496 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Toyota'
                AND model = 'Yaris'
        THEN
            '1197-1496 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mitsubishi'
                AND model = 'Van'
        THEN
            'not available'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Aston Martin'
                AND model = 'DBX'
        THEN
            '3982 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Chevrolet'
                AND model = 'Cruze'
        THEN
            '1399-1796 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mazda'
                AND model IN ('3' , '6')
        THEN
            '1496-2488 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Jaguar'
                AND model = 'XF'
        THEN
            '1999-2993 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Audi'
                AND model = 'Q3'
        THEN
            '1395-1984 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Lotus'
                AND model = 'Emira'
        THEN
            '1998 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mitsubishi'
                AND model = 'Montero'
        THEN
            '2442-2998 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'BAIC'
                AND model = 'X7'
        THEN
            '1498 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Audi'
                AND model = 'A7'
        THEN
            '2995 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Hongqi'
                AND model = 'HS5'
        THEN
            '1499 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Subaru'
                AND model = 'XV'
        THEN
            '1600-1995 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Chevrolet'
                AND model = 'Equinox'
        THEN
            '1373-1998 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Opel'
                AND model = 'Insignia'
        THEN
            '1364-1956 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Lincoln'
                AND model = 'MKC'
        THEN
            '1999 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Hongqi'
                AND model = 'E-HS9'
        THEN
            '1991 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Nissan'
                AND model = 'Maxima'
        THEN
            '2987-3498 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mazda'
                AND model = 'CX-5'
        THEN
            '1998-2488 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'MINI'
                AND model = 'Countryman'
        THEN
            '1499-1998 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Honda'
                AND model = 'Pilot'
        THEN
            '3471 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mitsubishi'
                AND model = 'Outlander'
        THEN
            '1998 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Chevrolet'
                AND model = 'Camaro'
        THEN
            '1998-6162 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Maserati'
                AND model = 'Quattroporte'
        THEN
            '2979-4691 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'BMW'
                AND model = 'M5'
        THEN
            '4395 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Ferrari'
                AND model = 'Testarossa'
        THEN
            '4942 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Toyota'
                AND model = 'Previa'
        THEN
            '2362 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Land Rover'
                AND model = 'Discovery Sport'
        THEN
            '1999-2996cc'
    END
WHERE
    engine_capacity_cc IS NULL
        AND brand IN ('Audi' , 'Lincoln',
        'Toyota',
        'Mitsubishi',
        'Land Rover',
        'Aston Martin',
        'Chevrolet',
        'Mazda',
        'Jaguar',
        'Lotus',
        'BAIC',
        'Maserati',
        'Hongqi',
        'Subaru',
        'Opel',
        'Nissan',
        'MINI',
        'Honda',
        'BMW',
        'Ferrari')
        AND model IN ('S6/RS6' , 'Avaiator',
        'Yaris',
        'Van',
        'DBX',
        'Cruze',
        'Discovery Sport',
        '3',
        '6',
        'XF',
        'Q3',
        'Emira',
        'Montero',
        'X7',
        'A7',
        'HS5',
        'XV',
        'Equinox',
        'Insignia',
        'MKC',
        'E-HS9',
        'Maxima',
        'CX-5',
        'Countryman',
        'Pilot',
        'Outlander',
        'Camaro',
        'Quattroporte',
        'M5',
        'Testarossa',
        'Previa');

UPDATE dubizzle_cars_dataset 
SET 
    engine_capacity_cc = CASE
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Ferrari'
                AND model = 'GTC4 Lusso T'
        THEN
            '3855 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Audi'
                AND model = 'S7/RS7'
        THEN
            '3996 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mercedes-Benz'
                AND model = 'CLS 450'
        THEN
            '2999 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Renault'
                AND model = 'Talisman'
        THEN
            '1197-1798 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Volkswagen'
                AND model = 'Scirocco'
        THEN
            '1390-1984 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Hyundai'
                AND model = 'Elantra'
        THEN
            '1493-1999 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mercedes-Benz'
                AND model = 'S-Class Coupe'
        THEN
            '2999-3982 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Hummer'
                AND model = 'H2'
        THEN
            '6162 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Dodge'
                AND model = 'Charger'
        THEN
            '3604-6417 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mercedes-Benz'
                AND model = 'EQE'
        THEN
            '0 cc (electric)'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Skoda'
                AND model = 'Karoq'
        THEN
            '999-1968 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'BMW'
                AND model = 'XM'
        THEN
            '4395 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Chevrolet'
                AND model = 'Malibu'
        THEN
            '1490-1998 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Renault'
                AND model = 'Symbol'
        THEN
            '898-1330 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Hyundai'
                AND model = 'i10'
        THEN
            '998 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Peugeot'
                AND model = '208 GT LINE'
        THEN
            '1199-1598 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Volvo'
                AND model = 'V-Class'
        THEN
            '1950 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Jetour'
                AND model = 'X90'
        THEN
            '1499 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Lexus'
                AND model LIKE 'IS%'
        THEN
            '1998-2499 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'BMW'
                AND model = 'M850i'
        THEN
            '4395 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'BMW'
                AND model = 'i8'
        THEN
            '1499 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mercedes-Maybach'
                AND model = 'S-Class'
        THEN
            '3982-5980 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Toyota'
                AND model = 'Land Cruiser 79 series'
        THEN
            '4461 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'BMW'
                AND model = 'X7'
        THEN
            '2998-6592 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Ferrari'
                AND model = '458'
        THEN
            '4499 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Chevrolet'
                AND model = 'Trax'
        THEN
            '1364 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Subaru'
                AND model = 'BRZ'
        THEN
            '1998 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mercedes-Benz'
                AND model = 'SLK-Class'
        THEN
            '1796-2996 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Land Rover'
                AND model = 'Discovery'
        THEN
            '1997-2996 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Lexus'
                AND model = 'RC'
        THEN
            '1998-4969 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Cadillac'
                AND model IN ('DTS' , 'De Ville')
        THEN
            '4565 cc'
    END
WHERE
    engine_capacity_cc IS NULL
        AND brand IN ('Ferrari' , 'Audi',
        'Mercedes-Benz',
        'Renault',
        'Volkswagen',
        'Hyundai',
        'Hummer',
        'Dodge',
        'Skoda',
        'BMW',
        'Chevrolet',
        'Peugeot',
        'Volvo',
        'Jetour',
        'Lexus',
        'Toyota',
        'Ferrari',
        'Chevrolet',
        'Subaru',
        'Mercedes-Benz',
        'Land Rover',
        'Cadillac')
        AND model IN ('GTC4 Lusso T' , 'S7/RS7',
        'CLS 450',
        'Talisman',
        'Scirocco',
        'Elantra',
        'S-Class Coupe',
        'H2',
        'Charger',
        'EQE',
        'Karoq',
        'XM',
        'Malibu',
        'Symbol',
        'i10',
        '208 GT LINE',
        'V-Class',
        'X90',
        'IS-Series',
        'M850i',
        'i8',
        'Maybach S-Class',
        'Land Cruiser 79 series',
        'X7',
        '458',
        'Trax',
        'BRZ',
        'SLK-Class',
        'Discovery',
        'RC',
        'DTS',
        'De Ville');

UPDATE dubizzle_cars_dataset 
SET 
    engine_capacity_cc = CASE
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Volvo'
                AND model = 'XC90'
        THEN
            '1969-2999 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mercedes-Benz'
                AND model = 'C43'
        THEN
            '2996 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Mercedes-Maybach'
                AND model = 'S-Class'
        THEN
            '3982-5980 cc'
        WHEN
            engine_capacity_cc IS NULL
                AND brand = 'Cadillac'
                AND model = 'DTS/De Ville'
        THEN
            '4565 cc'
    END
WHERE
    engine_capacity_cc IS NULL
        AND brand IN ('Volvo' , 'Mercedes-Benz',
        'Mercedes-Maybach',
        'Cadillac')
        AND model IN ('XC90' , 'C43', 'S-Class', 'DTS/De Ville');

UPDATE dubizzle_cars_dataset 
SET 
    engine_capacity_cc = 'Unknown'
WHERE
    model = 'Other'
        AND engine_capacity_cc IS NULL;
commit;

SELECT DISTINCT
    brand, model
FROM
    dubizzle_cars_dataset
WHERE
    engine_capacity_cc IS NULL;

SELECT 
    *
FROM
    dubizzle_cars_dataset;

-- get all raws in which any column is null or empty
SELECT 
    CONCAT('SELECT * FROM dubizzle_cars_dataset WHERE ',
            GROUP_CONCAT(CONCAT(column_name, ' IS NULL')
                SEPARATOR ' OR '))
INTO @sql FROM
    information_schema.columns
WHERE
    table_name = 'dubizzle_cars_dataset'
        AND table_schema = 'dubizzle_car_dataset';

SELECT @sql;

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT 
    *
FROM
    dubizzle_cars_dataset
WHERE
    no_of_cylinders = 0
        AND fuel_type = 'Petrol';
UPDATE dubizzle_cars_dataset 
SET 
    kilometers = 0
WHERE
    kilometers IS NULL;
UPDATE dubizzle_cars_dataset 
SET 
    vehicle_age_years = 0
WHERE
    vehicle_age_years IS NULL;
UPDATE dubizzle_cars_dataset 
SET 
    seating_capacity = 2
WHERE
    seating_capacity IS NULL;
UPDATE dubizzle_cars_dataset 
SET 
    horsepower = 'Unknown'
WHERE
    horsepower IS NULL;
UPDATE dubizzle_cars_dataset 
SET 
    no_of_cylinders = '0'
WHERE
    no_of_cylinders IS NULL
        AND fuel_type = 'Electric'; -- electric cars

ALTER TABLE dubizzle_cars_dataset MODIFY COLUMN no_of_cylinders VARCHAR(255);

UPDATE dubizzle_cars_dataset 
SET 
    no_of_cylinders = 'Unknown'
WHERE
    no_of_cylinders = 0
        AND fuel_type = 'Petrol';
DELETE FROM dubizzle_cars_dataset 
WHERE
    trim = 'Unknown';

UPDATE dubizzle_cars_dataset 
SET 
    area_name = location_name
WHERE
    area_name IS NULL;
UPDATE dubizzle_cars_dataset 
SET 
    location_name = area_name
WHERE
    location_name IS NULL;

UPDATE dubizzle_cars_dataset 
SET 
    location_name = 'Unknown',
    area_name = 'Unknown'
WHERE
    location_name IS NULL
        AND area_name IS NULL;
        
commit;


select * from dubizzle_cars_dataset