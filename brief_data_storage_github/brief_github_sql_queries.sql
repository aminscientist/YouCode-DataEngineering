USE GitHubDB;

--------------------------------
----test l'integrite de donn�es
--------------------------------

-- Requ�te pour compter le nombre total de d�p�ts dans la table Repositories
SELECT COUNT(*) AS total_depots
FROM Repositories;

-- Requ�te pour obtenir les d�p�ts les plus populaires en termes d'�toiles
SELECT full_name, stars
FROM Repositories
ORDER BY stars DESC
OFFSET 0 ROWS
FETCH NEXT 10 ROWS ONLY;

-- Requ�te pour obtenir les contributeurs ayant effectu� le plus grand nombre de contributions
SELECT contributor, total_contributions
FROM (
    SELECT c.contributor, SUM(ct.contributions) AS total_contributions,
           ROW_NUMBER() OVER (ORDER BY SUM(ct.contributions) DESC) AS rn
    FROM Contributors c
    INNER JOIN Contributions ct ON c.id_contributor = ct.id_contributor
    GROUP BY c.contributor
) ranked
WHERE rn <= 10;

-- Requ�te pour obtenir les langages de programmation les plus utilis�s dans les d�p�ts avec leurs noms
SELECT l.language, total_depots
FROM (
    SELECT r.id_language, COUNT(*) AS total_depots,
           ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rn
    FROM Repositories r
    GROUP BY r.id_language
) ranked
INNER JOIN Languages l ON ranked.id_language = l.id_language
WHERE rn <= 10;



-- Requ�te pour obtenir les sujets les plus populaires parmi les d�p�ts
SELECT topic, total_depots
FROM (
    SELECT t.topic, COUNT(*) AS total_depots,
           ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rn
    FROM Topics_repo tr
    INNER JOIN Topics t ON tr.id_topic = t.id_topic
    GROUP BY t.topic
) ranked
WHERE rn <= 10;

------------------------------------------------------
------test l'indexation et la performance de recherche
------------------------------------------------------

-- Temps avant l'indexation
SET STATISTICS TIME ON;
SET STATISTICS IO ON;

SELECT DISTINCT c.contributor
FROM Contributors c
INNER JOIN Contributions ct ON c.id_contributor = ct.id_contributor
INNER JOIN Topics_repo tr ON ct.id_repo = tr.id_repo
INNER JOIN Topics t ON tr.id_topic = t.id_topic
WHERE t.topic = 'flutter';

SET STATISTICS TIME OFF;
SET STATISTICS IO OFF;

-- Cr�er les index n�cessaires
CREATE INDEX idx_contributors_id_contributor ON Contributors (id_contributor);
CREATE INDEX idx_contributions_id_contributor ON Contributions (id_contributor);
CREATE INDEX idx_topics_repo_id_topic ON Topics_repo (id_topic);
CREATE INDEX idx_topics_repo_id_repo ON Topics_repo (id_repo);
CREATE INDEX idx_topics_id_topic ON Topics (id_topic);


























