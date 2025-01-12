-- Projet SQL - Nettoyage des Données

-- https://www.kaggle.com/datasets/swaptr/layoffs-2022

SELECT * 
FROM world_layoffs.layoffs;

-- La première chose à faire est de créer une table staging. C'est celle dans laquelle nous travaillerons pour nettoyer les données. 
-- Nous voulons également conserver une table avec les données brutes au cas où quelque chose se passe mal.
CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

INSERT INTO layoffs_staging 
SELECT * FROM world_layoffs.layoffs;

-- Lorsque nous nettoyons les données, nous suivons généralement ces étapes :
-- 1. Vérifier les doublons et les supprimer.
-- 2. Standardiser les données et corriger les erreurs.
-- 3. Examiner les valeurs nulles et voir comment les traiter.
-- 4. Supprimer les colonnes et lignes inutiles selon différents critères.

-- 1. Supprimer les doublons

-- Commençons par vérifier s'il y a des doublons.

SELECT *
FROM world_layoffs.layoffs_staging
;

SELECT company, industry, total_laid_off, `date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off, `date`
		) AS row_num
	FROM 
		world_layoffs.layoffs_staging;

SELECT *
FROM (
	SELECT company, industry, total_laid_off, `date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off, `date`
		) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;

-- Vérifions les données pour une entreprise spécifique, Oda, pour confirmer.
SELECT *
FROM world_layoffs.layoffs_staging
WHERE company = 'Oda'
;

-- Il semble que toutes ces entrées soient légitimes et ne doivent pas être supprimées. 
-- Nous devons examiner chaque ligne en détail pour être précis.

-- Voici les vrais doublons que nous souhaitons supprimer.
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
		) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;

-- Ce sont ceux que nous voulons supprimer si le numéro de ligne (row_num) est supérieur à 1.

-- Voici une méthode où nous crééons un CTE (Common Table Expression) pour identifier les doublons à supprimer.
WITH DELETE_CTE AS 
(
	SELECT *
	FROM (
		SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
			ROW_NUMBER() OVER (
				PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
			) AS row_num
		FROM 
			world_layoffs.layoffs_staging
	) duplicates
	WHERE 
		row_num > 1
)
DELETE
FROM DELETE_CTE;

-- Une autre solution consiste à ajouter une nouvelle colonne avec les numéros de ligne (row numbers), 
-- puis à supprimer les lignes avec un numéro supérieur à 1, avant de supprimer la colonne ajoutée.

ALTER TABLE world_layoffs.layoffs_staging ADD row_num INT;

SELECT *
FROM world_layoffs.layoffs_staging;

CREATE TABLE `world_layoffs`.`layoffs_staging2` (
	`company` TEXT,
	`location` TEXT,
	`industry` TEXT,
	`total_laid_off` INT,
	`percentage_laid_off` TEXT,
	`date` TEXT,
	`stage` TEXT,
	`country` TEXT,
	`funds_raised_millions` INT,
	row_num INT
);

INSERT INTO `world_layoffs`.`layoffs_staging2`
(`company`, `location`, `industry`, `total_laid_off`, `percentage_laid_off`, `date`, `stage`, `country`, `funds_raised_millions`, `row_num`)
SELECT `company`, `location`, `industry`, `total_laid_off`, `percentage_laid_off`, `date`, `stage`, `country`, `funds_raised_millions`,
	ROW_NUMBER() OVER (
		PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
	) AS row_num
FROM 
	world_layoffs.layoffs_staging;

-- Maintenant, supprimons les lignes où le numéro de ligne (row_num) est supérieur ou égal à 2.

DELETE FROM world_layoffs.layoffs_staging2
WHERE row_num >= 2;

-- 2. Standardiser les données

SELECT * 
FROM world_layoffs.layoffs_staging2;

-- En examinant la colonne "industry", nous constatons qu'il y a des valeurs nulles ou vides. Analysons cela.
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- etc.
