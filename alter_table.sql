/* titleakas*/
ALTER TABLE titleakas
ALTER COLUMN ordering TYPE INTEGER
USING ordering::integer,

ALTER COLUMN "isOriginalTitle" TYPE Boolean
USING (CASE 
    WHEN "isOriginalTitle" = '1' THEN true 
    ELSE false
END);

/* titlebasics*/
ALTER TABLE titlebasics

ALTER COLUMN "isAdult" TYPE boolean
USING (CASE 
    WHEN "isAdult" = '1' THEN true 
    ELSE false 
END),

ALTER COLUMN "startYear" TYPE integer
USING (CASE 
    WHEN "startYear" IS NULL THEN NULL 
    ELSE "startYear"::integer
END),

ALTER COLUMN "endYear" TYPE integer
USING (CASE 
    WHEN "endYear" IS NULL THEN NULL 
    ELSE "endYear"::integer
END),

ALTER COLUMN "runtimeMinutes" TYPE INTEGER
USING (CASE
   WHEN "runtimeMinutes" ~ '^\d+$' THEN "runtimeMinutes"::INTEGER
   ELSE 0
END);

/* titleepisode*/
ALTER TABLE titleepisode

ALTER COLUMN "seasonNumber" TYPE integer
USING "seasonNumber"::integer,

ALTER COLUMN "episodeNumber" TYPE integer
USING "episodeNumber"::integer;

/* titleprincipals*/
ALTER TABLE titleprincipals

ALTER COLUMN "ordering" TYPE integer
USING "ordering"::integer;

/* namebasics*/
ALTER TABLE namebasics

ALTER COLUMN "birthYear" TYPE integer
USING (CASE 
    WHEN "birthYear" IS NULL THEN NULL 
    ELSE "birthYear"::integer 
END),

ALTER COLUMN "deathYear" TYPE integer
USING (CASE 
    WHEN "deathYear" IS NULL THEN NULL 
    ELSE "deathYear"::integer
END);
