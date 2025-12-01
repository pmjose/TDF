-- ============================================================================
-- TDF DATA PLATFORM - SEED DATA: FRANCE GEOGRAPHY
-- ============================================================================
-- 13 Metropolitan Regions, 96 Departments
-- ============================================================================

USE ROLE SYSADMIN;
USE WAREHOUSE TDF_WH;
USE DATABASE TDF_DATA_PLATFORM;
USE SCHEMA CORE;

-- ============================================================================
-- TRUNCATE GEOGRAPHY TABLES (Idempotent - safe to re-run)
-- Order matters due to foreign key dependencies
-- ============================================================================

TRUNCATE TABLE IF EXISTS CALENDAR;
TRUNCATE TABLE IF EXISTS DEPARTMENTS;
TRUNCATE TABLE IF EXISTS REGIONS;

-- ============================================================================
-- REGIONS (13 Metropolitan French Regions)
-- ============================================================================

INSERT INTO REGIONS (REGION_ID, REGION_NAME, REGION_CODE, CAPITAL_CITY, LATITUDE, LONGITUDE, POPULATION, AREA_KM2, SITE_DENSITY_TARGET)
VALUES
    ('REG-IDF', 'Île-de-France', 'IDF', 'Paris', 48.8566, 2.3522, 12278210, 12012, 150),
    ('REG-ARA', 'Auvergne-Rhône-Alpes', 'ARA', 'Lyon', 45.7640, 4.8357, 8078652, 69711, 80),
    ('REG-NAQ', 'Nouvelle-Aquitaine', 'NAQ', 'Bordeaux', 44.8378, -0.5792, 6010289, 84061, 50),
    ('REG-OCC', 'Occitanie', 'OCC', 'Toulouse', 43.6047, 1.4442, 5973969, 72724, 55),
    ('REG-HDF', 'Hauts-de-France', 'HDF', 'Lille', 50.6292, 3.0573, 6009976, 31813, 100),
    ('REG-GES', 'Grand Est', 'GES', 'Strasbourg', 48.5734, 7.7521, 5561287, 57441, 65),
    ('REG-PAC', 'Provence-Alpes-Côte d''Azur', 'PAC', 'Marseille', 43.2965, 5.3698, 5098037, 31400, 90),
    ('REG-PDL', 'Pays de la Loire', 'PDL', 'Nantes', 47.2184, -1.5536, 3838557, 32082, 70),
    ('REG-BRE', 'Bretagne', 'BRE', 'Rennes', 48.1173, -1.6778, 3373835, 27208, 75),
    ('REG-NOR', 'Normandie', 'NOR', 'Rouen', 49.4432, 1.0999, 3325522, 29906, 65),
    ('REG-BFC', 'Bourgogne-Franche-Comté', 'BFC', 'Dijon', 47.3220, 5.0415, 2807807, 47784, 40),
    ('REG-CVL', 'Centre-Val de Loire', 'CVL', 'Orléans', 47.9029, 1.9093, 2576252, 39151, 45),
    ('REG-COR', 'Corse', 'COR', 'Ajaccio', 41.9192, 8.7386, 344679, 8680, 30);

-- ============================================================================
-- DEPARTMENTS (96 Metropolitan French Departments)
-- ============================================================================

-- Île-de-France (8 departments)
INSERT INTO DEPARTMENTS (DEPARTMENT_ID, DEPARTMENT_CODE, DEPARTMENT_NAME, REGION_ID, PREFECTURE, LATITUDE, LONGITUDE, POPULATION, AREA_KM2, IS_URBAN)
VALUES
    ('DEP-75', '75', 'Paris', 'REG-IDF', 'Paris', 48.8566, 2.3522, 2161000, 105, TRUE),
    ('DEP-77', '77', 'Seine-et-Marne', 'REG-IDF', 'Melun', 48.5421, 2.6553, 1421000, 5915, FALSE),
    ('DEP-78', '78', 'Yvelines', 'REG-IDF', 'Versailles', 48.8035, 2.1266, 1448000, 2285, TRUE),
    ('DEP-91', '91', 'Essonne', 'REG-IDF', 'Évry', 48.6247, 2.4453, 1306000, 1804, TRUE),
    ('DEP-92', '92', 'Hauts-de-Seine', 'REG-IDF', 'Nanterre', 48.8924, 2.2071, 1624000, 176, TRUE),
    ('DEP-93', '93', 'Seine-Saint-Denis', 'REG-IDF', 'Bobigny', 48.9096, 2.4391, 1644000, 236, TRUE),
    ('DEP-94', '94', 'Val-de-Marne', 'REG-IDF', 'Créteil', 48.7794, 2.4556, 1407000, 245, TRUE),
    ('DEP-95', '95', 'Val-d''Oise', 'REG-IDF', 'Cergy', 49.0363, 2.0632, 1249000, 1246, TRUE);

-- Auvergne-Rhône-Alpes (12 departments)
INSERT INTO DEPARTMENTS (DEPARTMENT_ID, DEPARTMENT_CODE, DEPARTMENT_NAME, REGION_ID, PREFECTURE, LATITUDE, LONGITUDE, POPULATION, AREA_KM2, IS_URBAN)
VALUES
    ('DEP-01', '01', 'Ain', 'REG-ARA', 'Bourg-en-Bresse', 46.2056, 5.2255, 657000, 5762, FALSE),
    ('DEP-03', '03', 'Allier', 'REG-ARA', 'Moulins', 46.5670, 3.3328, 337000, 7340, FALSE),
    ('DEP-07', '07', 'Ardèche', 'REG-ARA', 'Privas', 44.7355, 4.5986, 328000, 5529, FALSE),
    ('DEP-15', '15', 'Cantal', 'REG-ARA', 'Aurillac', 44.9308, 2.4450, 145000, 5726, FALSE),
    ('DEP-26', '26', 'Drôme', 'REG-ARA', 'Valence', 44.9334, 4.8924, 519000, 6530, FALSE),
    ('DEP-38', '38', 'Isère', 'REG-ARA', 'Grenoble', 45.1885, 5.7245, 1271000, 7431, TRUE),
    ('DEP-42', '42', 'Loire', 'REG-ARA', 'Saint-Étienne', 45.4397, 4.3872, 765000, 4781, TRUE),
    ('DEP-43', '43', 'Haute-Loire', 'REG-ARA', 'Le Puy-en-Velay', 45.0426, 3.8853, 227000, 4977, FALSE),
    ('DEP-63', '63', 'Puy-de-Dôme', 'REG-ARA', 'Clermont-Ferrand', 45.7772, 3.0870, 662000, 7970, TRUE),
    ('DEP-69', '69', 'Rhône', 'REG-ARA', 'Lyon', 45.7640, 4.8357, 1876000, 3249, TRUE),
    ('DEP-73', '73', 'Savoie', 'REG-ARA', 'Chambéry', 45.5646, 5.9178, 439000, 6028, FALSE),
    ('DEP-74', '74', 'Haute-Savoie', 'REG-ARA', 'Annecy', 45.8992, 6.1294, 826000, 4388, FALSE);

-- Nouvelle-Aquitaine (12 departments)
INSERT INTO DEPARTMENTS (DEPARTMENT_ID, DEPARTMENT_CODE, DEPARTMENT_NAME, REGION_ID, PREFECTURE, LATITUDE, LONGITUDE, POPULATION, AREA_KM2, IS_URBAN)
VALUES
    ('DEP-16', '16', 'Charente', 'REG-NAQ', 'Angoulême', 45.6493, 0.1560, 352000, 5956, FALSE),
    ('DEP-17', '17', 'Charente-Maritime', 'REG-NAQ', 'La Rochelle', 46.1603, -1.1511, 651000, 6864, FALSE),
    ('DEP-19', '19', 'Corrèze', 'REG-NAQ', 'Tulle', 45.2670, 1.7697, 241000, 5857, FALSE),
    ('DEP-23', '23', 'Creuse', 'REG-NAQ', 'Guéret', 46.1698, 1.8718, 118000, 5565, FALSE),
    ('DEP-24', '24', 'Dordogne', 'REG-NAQ', 'Périgueux', 45.1847, 0.7216, 413000, 9060, FALSE),
    ('DEP-33', '33', 'Gironde', 'REG-NAQ', 'Bordeaux', 44.8378, -0.5792, 1623000, 10000, TRUE),
    ('DEP-40', '40', 'Landes', 'REG-NAQ', 'Mont-de-Marsan', 43.8898, -0.4997, 413000, 9243, FALSE),
    ('DEP-47', '47', 'Lot-et-Garonne', 'REG-NAQ', 'Agen', 44.2033, 0.6166, 331000, 5361, FALSE),
    ('DEP-64', '64', 'Pyrénées-Atlantiques', 'REG-NAQ', 'Pau', 43.2951, -0.3708, 687000, 7645, FALSE),
    ('DEP-79', '79', 'Deux-Sèvres', 'REG-NAQ', 'Niort', 46.3235, -0.4646, 374000, 5999, FALSE),
    ('DEP-86', '86', 'Vienne', 'REG-NAQ', 'Poitiers', 46.5802, 0.3404, 438000, 6990, FALSE),
    ('DEP-87', '87', 'Haute-Vienne', 'REG-NAQ', 'Limoges', 45.8336, 1.2611, 374000, 5520, TRUE);

-- Occitanie (13 departments)
INSERT INTO DEPARTMENTS (DEPARTMENT_ID, DEPARTMENT_CODE, DEPARTMENT_NAME, REGION_ID, PREFECTURE, LATITUDE, LONGITUDE, POPULATION, AREA_KM2, IS_URBAN)
VALUES
    ('DEP-09', '09', 'Ariège', 'REG-OCC', 'Foix', 42.9658, 1.6078, 153000, 4890, FALSE),
    ('DEP-11', '11', 'Aude', 'REG-OCC', 'Carcassonne', 43.2130, 2.3491, 374000, 6139, FALSE),
    ('DEP-12', '12', 'Aveyron', 'REG-OCC', 'Rodez', 44.3497, 2.5753, 279000, 8735, FALSE),
    ('DEP-30', '30', 'Gard', 'REG-OCC', 'Nîmes', 43.8367, 4.3601, 748000, 5853, TRUE),
    ('DEP-31', '31', 'Haute-Garonne', 'REG-OCC', 'Toulouse', 43.6047, 1.4442, 1400000, 6309, TRUE),
    ('DEP-32', '32', 'Gers', 'REG-OCC', 'Auch', 43.6465, 0.5855, 191000, 6257, FALSE),
    ('DEP-34', '34', 'Hérault', 'REG-OCC', 'Montpellier', 43.6108, 3.8767, 1176000, 6101, TRUE),
    ('DEP-46', '46', 'Lot', 'REG-OCC', 'Cahors', 44.4475, 1.4370, 174000, 5217, FALSE),
    ('DEP-48', '48', 'Lozère', 'REG-OCC', 'Mende', 44.5181, 3.4985, 76000, 5167, FALSE),
    ('DEP-65', '65', 'Hautes-Pyrénées', 'REG-OCC', 'Tarbes', 43.2329, -0.0782, 229000, 4464, FALSE),
    ('DEP-66', '66', 'Pyrénées-Orientales', 'REG-OCC', 'Perpignan', 42.6887, 2.8948, 479000, 4116, FALSE),
    ('DEP-81', '81', 'Tarn', 'REG-OCC', 'Albi', 43.9298, 2.1480, 389000, 5758, FALSE),
    ('DEP-82', '82', 'Tarn-et-Garonne', 'REG-OCC', 'Montauban', 44.0176, 1.3547, 262000, 3718, FALSE);

-- Hauts-de-France (5 departments)
INSERT INTO DEPARTMENTS (DEPARTMENT_ID, DEPARTMENT_CODE, DEPARTMENT_NAME, REGION_ID, PREFECTURE, LATITUDE, LONGITUDE, POPULATION, AREA_KM2, IS_URBAN)
VALUES
    ('DEP-02', '02', 'Aisne', 'REG-HDF', 'Laon', 49.5632, 3.6200, 534000, 7369, FALSE),
    ('DEP-59', '59', 'Nord', 'REG-HDF', 'Lille', 50.6292, 3.0573, 2608000, 5743, TRUE),
    ('DEP-60', '60', 'Oise', 'REG-HDF', 'Beauvais', 49.4294, 2.0800, 829000, 5860, TRUE),
    ('DEP-62', '62', 'Pas-de-Calais', 'REG-HDF', 'Arras', 50.2916, 2.7775, 1470000, 6671, TRUE),
    ('DEP-80', '80', 'Somme', 'REG-HDF', 'Amiens', 49.8941, 2.2958, 572000, 6170, FALSE);

-- Grand Est (10 departments)
INSERT INTO DEPARTMENTS (DEPARTMENT_ID, DEPARTMENT_CODE, DEPARTMENT_NAME, REGION_ID, PREFECTURE, LATITUDE, LONGITUDE, POPULATION, AREA_KM2, IS_URBAN)
VALUES
    ('DEP-08', '08', 'Ardennes', 'REG-GES', 'Charleville-Mézières', 49.7597, 4.7172, 274000, 5229, FALSE),
    ('DEP-10', '10', 'Aube', 'REG-GES', 'Troyes', 48.2973, 4.0744, 310000, 6004, FALSE),
    ('DEP-51', '51', 'Marne', 'REG-GES', 'Châlons-en-Champagne', 48.9566, 4.3631, 568000, 8162, FALSE),
    ('DEP-52', '52', 'Haute-Marne', 'REG-GES', 'Chaumont', 48.1139, 5.1388, 175000, 6211, FALSE),
    ('DEP-54', '54', 'Meurthe-et-Moselle', 'REG-GES', 'Nancy', 48.6921, 6.1844, 733000, 5246, TRUE),
    ('DEP-55', '55', 'Meuse', 'REG-GES', 'Bar-le-Duc', 48.7731, 5.1607, 187000, 6211, FALSE),
    ('DEP-57', '57', 'Moselle', 'REG-GES', 'Metz', 49.1193, 6.1757, 1046000, 6216, TRUE),
    ('DEP-67', '67', 'Bas-Rhin', 'REG-GES', 'Strasbourg', 48.5734, 7.7521, 1140000, 4755, TRUE),
    ('DEP-68', '68', 'Haut-Rhin', 'REG-GES', 'Colmar', 48.0794, 7.3585, 764000, 3525, TRUE),
    ('DEP-88', '88', 'Vosges', 'REG-GES', 'Épinal', 48.1725, 6.4498, 367000, 5874, FALSE);

-- Provence-Alpes-Côte d'Azur (6 departments)
INSERT INTO DEPARTMENTS (DEPARTMENT_ID, DEPARTMENT_CODE, DEPARTMENT_NAME, REGION_ID, PREFECTURE, LATITUDE, LONGITUDE, POPULATION, AREA_KM2, IS_URBAN)
VALUES
    ('DEP-04', '04', 'Alpes-de-Haute-Provence', 'REG-PAC', 'Digne-les-Bains', 44.0932, 6.2356, 164000, 6925, FALSE),
    ('DEP-05', '05', 'Hautes-Alpes', 'REG-PAC', 'Gap', 44.5594, 6.0780, 141000, 5549, FALSE),
    ('DEP-06', '06', 'Alpes-Maritimes', 'REG-PAC', 'Nice', 43.7102, 7.2620, 1083000, 4299, TRUE),
    ('DEP-13', '13', 'Bouches-du-Rhône', 'REG-PAC', 'Marseille', 43.2965, 5.3698, 2034000, 5087, TRUE),
    ('DEP-83', '83', 'Var', 'REG-PAC', 'Toulon', 43.1242, 5.9280, 1076000, 5973, TRUE),
    ('DEP-84', '84', 'Vaucluse', 'REG-PAC', 'Avignon', 43.9493, 4.8055, 559000, 3567, TRUE);

-- Pays de la Loire (5 departments)
INSERT INTO DEPARTMENTS (DEPARTMENT_ID, DEPARTMENT_CODE, DEPARTMENT_NAME, REGION_ID, PREFECTURE, LATITUDE, LONGITUDE, POPULATION, AREA_KM2, IS_URBAN)
VALUES
    ('DEP-44', '44', 'Loire-Atlantique', 'REG-PDL', 'Nantes', 47.2184, -1.5536, 1429000, 6815, TRUE),
    ('DEP-49', '49', 'Maine-et-Loire', 'REG-PDL', 'Angers', 47.4784, -0.5632, 818000, 7166, TRUE),
    ('DEP-53', '53', 'Mayenne', 'REG-PDL', 'Laval', 48.0736, -0.7698, 307000, 5175, FALSE),
    ('DEP-72', '72', 'Sarthe', 'REG-PDL', 'Le Mans', 47.9960, 0.1996, 566000, 6206, TRUE),
    ('DEP-85', '85', 'Vendée', 'REG-PDL', 'La Roche-sur-Yon', 46.6705, -1.4266, 685000, 6720, FALSE);

-- Bretagne (4 departments)
INSERT INTO DEPARTMENTS (DEPARTMENT_ID, DEPARTMENT_CODE, DEPARTMENT_NAME, REGION_ID, PREFECTURE, LATITUDE, LONGITUDE, POPULATION, AREA_KM2, IS_URBAN)
VALUES
    ('DEP-22', '22', 'Côtes-d''Armor', 'REG-BRE', 'Saint-Brieuc', 48.5141, -2.7615, 598000, 6878, FALSE),
    ('DEP-29', '29', 'Finistère', 'REG-BRE', 'Quimper', 47.9960, -4.0999, 909000, 6733, FALSE),
    ('DEP-35', '35', 'Ille-et-Vilaine', 'REG-BRE', 'Rennes', 48.1173, -1.6778, 1079000, 6775, TRUE),
    ('DEP-56', '56', 'Morbihan', 'REG-BRE', 'Vannes', 47.6586, -2.7609, 759000, 6823, FALSE);

-- Normandie (5 departments)
INSERT INTO DEPARTMENTS (DEPARTMENT_ID, DEPARTMENT_CODE, DEPARTMENT_NAME, REGION_ID, PREFECTURE, LATITUDE, LONGITUDE, POPULATION, AREA_KM2, IS_URBAN)
VALUES
    ('DEP-14', '14', 'Calvados', 'REG-NOR', 'Caen', 49.1829, -0.3707, 694000, 5548, TRUE),
    ('DEP-27', '27', 'Eure', 'REG-NOR', 'Évreux', 49.0269, 1.1508, 601000, 6040, FALSE),
    ('DEP-50', '50', 'Manche', 'REG-NOR', 'Saint-Lô', 49.1166, -1.0906, 496000, 5938, FALSE),
    ('DEP-61', '61', 'Orne', 'REG-NOR', 'Alençon', 48.4320, 0.0912, 283000, 6103, FALSE),
    ('DEP-76', '76', 'Seine-Maritime', 'REG-NOR', 'Rouen', 49.4432, 1.0999, 1254000, 6278, TRUE);

-- Bourgogne-Franche-Comté (8 departments)
INSERT INTO DEPARTMENTS (DEPARTMENT_ID, DEPARTMENT_CODE, DEPARTMENT_NAME, REGION_ID, PREFECTURE, LATITUDE, LONGITUDE, POPULATION, AREA_KM2, IS_URBAN)
VALUES
    ('DEP-21', '21', 'Côte-d''Or', 'REG-BFC', 'Dijon', 47.3220, 5.0415, 534000, 8763, TRUE),
    ('DEP-25', '25', 'Doubs', 'REG-BFC', 'Besançon', 47.2378, 6.0241, 543000, 5234, TRUE),
    ('DEP-39', '39', 'Jura', 'REG-BFC', 'Lons-le-Saunier', 46.6758, 5.5511, 260000, 4999, FALSE),
    ('DEP-58', '58', 'Nièvre', 'REG-BFC', 'Nevers', 46.9900, 3.1593, 207000, 6817, FALSE),
    ('DEP-70', '70', 'Haute-Saône', 'REG-BFC', 'Vesoul', 47.6197, 6.1549, 237000, 5360, FALSE),
    ('DEP-71', '71', 'Saône-et-Loire', 'REG-BFC', 'Mâcon', 46.3069, 4.8286, 553000, 8575, FALSE),
    ('DEP-89', '89', 'Yonne', 'REG-BFC', 'Auxerre', 47.7979, 3.5714, 338000, 7427, FALSE),
    ('DEP-90', '90', 'Territoire de Belfort', 'REG-BFC', 'Belfort', 47.6383, 6.8628, 143000, 609, TRUE);

-- Centre-Val de Loire (6 departments)
INSERT INTO DEPARTMENTS (DEPARTMENT_ID, DEPARTMENT_CODE, DEPARTMENT_NAME, REGION_ID, PREFECTURE, LATITUDE, LONGITUDE, POPULATION, AREA_KM2, IS_URBAN)
VALUES
    ('DEP-18', '18', 'Cher', 'REG-CVL', 'Bourges', 47.0810, 2.3988, 304000, 7235, FALSE),
    ('DEP-28', '28', 'Eure-et-Loir', 'REG-CVL', 'Chartres', 48.4439, 1.4890, 433000, 5880, FALSE),
    ('DEP-36', '36', 'Indre', 'REG-CVL', 'Châteauroux', 46.8104, 1.6914, 222000, 6791, FALSE),
    ('DEP-37', '37', 'Indre-et-Loire', 'REG-CVL', 'Tours', 47.3941, 0.6848, 610000, 6127, TRUE),
    ('DEP-41', '41', 'Loir-et-Cher', 'REG-CVL', 'Blois', 47.5861, 1.3359, 331000, 6343, FALSE),
    ('DEP-45', '45', 'Loiret', 'REG-CVL', 'Orléans', 47.9029, 1.9093, 679000, 6775, TRUE);

-- Corse (2 departments)
INSERT INTO DEPARTMENTS (DEPARTMENT_ID, DEPARTMENT_CODE, DEPARTMENT_NAME, REGION_ID, PREFECTURE, LATITUDE, LONGITUDE, POPULATION, AREA_KM2, IS_URBAN)
VALUES
    ('DEP-2A', '2A', 'Corse-du-Sud', 'REG-COR', 'Ajaccio', 41.9192, 8.7386, 158000, 4014, FALSE),
    ('DEP-2B', '2B', 'Haute-Corse', 'REG-COR', 'Bastia', 42.6973, 9.4509, 181000, 4666, FALSE);

-- ============================================================================
-- CALENDAR (June 2025 to June 2027)
-- ============================================================================

INSERT INTO CALENDAR (DATE_KEY, YEAR, QUARTER, MONTH, MONTH_NAME, WEEK_OF_YEAR, DAY_OF_MONTH, DAY_OF_WEEK, DAY_NAME, IS_WEEKEND, IS_FRENCH_HOLIDAY, HOLIDAY_NAME, FISCAL_YEAR, FISCAL_QUARTER)
SELECT 
    DATEADD(DAY, SEQ4(), '2025-06-01')::DATE AS DATE_KEY,
    YEAR(DATEADD(DAY, SEQ4(), '2025-06-01')) AS YEAR,
    QUARTER(DATEADD(DAY, SEQ4(), '2025-06-01')) AS QUARTER,
    MONTH(DATEADD(DAY, SEQ4(), '2025-06-01')) AS MONTH,
    MONTHNAME(DATEADD(DAY, SEQ4(), '2025-06-01')) AS MONTH_NAME,
    WEEKOFYEAR(DATEADD(DAY, SEQ4(), '2025-06-01')) AS WEEK_OF_YEAR,
    DAY(DATEADD(DAY, SEQ4(), '2025-06-01')) AS DAY_OF_MONTH,
    DAYOFWEEK(DATEADD(DAY, SEQ4(), '2025-06-01')) AS DAY_OF_WEEK,
    DAYNAME(DATEADD(DAY, SEQ4(), '2025-06-01')) AS DAY_NAME,
    CASE WHEN DAYOFWEEK(DATEADD(DAY, SEQ4(), '2025-06-01')) IN (0, 6) THEN TRUE ELSE FALSE END AS IS_WEEKEND,
    FALSE AS IS_FRENCH_HOLIDAY,
    NULL AS HOLIDAY_NAME,
    YEAR(DATEADD(DAY, SEQ4(), '2025-06-01')) AS FISCAL_YEAR,
    QUARTER(DATEADD(DAY, SEQ4(), '2025-06-01')) AS FISCAL_QUARTER
FROM TABLE(GENERATOR(ROWCOUNT => 762));  -- ~2 years

-- Update French holidays
UPDATE CALENDAR SET IS_FRENCH_HOLIDAY = TRUE, HOLIDAY_NAME = 'Jour de l''An' WHERE DATE_KEY IN ('2026-01-01', '2027-01-01');
UPDATE CALENDAR SET IS_FRENCH_HOLIDAY = TRUE, HOLIDAY_NAME = 'Fête du Travail' WHERE DATE_KEY IN ('2026-05-01', '2027-05-01');
UPDATE CALENDAR SET IS_FRENCH_HOLIDAY = TRUE, HOLIDAY_NAME = 'Victoire 1945' WHERE DATE_KEY IN ('2026-05-08', '2027-05-08');
UPDATE CALENDAR SET IS_FRENCH_HOLIDAY = TRUE, HOLIDAY_NAME = 'Fête Nationale' WHERE DATE_KEY IN ('2025-07-14', '2026-07-14');
UPDATE CALENDAR SET IS_FRENCH_HOLIDAY = TRUE, HOLIDAY_NAME = 'Assomption' WHERE DATE_KEY IN ('2025-08-15', '2026-08-15');
UPDATE CALENDAR SET IS_FRENCH_HOLIDAY = TRUE, HOLIDAY_NAME = 'Toussaint' WHERE DATE_KEY IN ('2025-11-01', '2026-11-01');
UPDATE CALENDAR SET IS_FRENCH_HOLIDAY = TRUE, HOLIDAY_NAME = 'Armistice' WHERE DATE_KEY IN ('2025-11-11', '2026-11-11');
UPDATE CALENDAR SET IS_FRENCH_HOLIDAY = TRUE, HOLIDAY_NAME = 'Noël' WHERE DATE_KEY IN ('2025-12-25', '2026-12-25');

SELECT 'FRANCE GEOGRAPHY DATA SEEDED' AS STATUS, 
       (SELECT COUNT(*) FROM REGIONS) AS REGIONS_COUNT,
       (SELECT COUNT(*) FROM DEPARTMENTS) AS DEPARTMENTS_COUNT,
       (SELECT COUNT(*) FROM CALENDAR) AS CALENDAR_DAYS;

