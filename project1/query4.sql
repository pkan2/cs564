SELECT ItemID
FROM Items
WHERE CURRENTLY = 
(SELECT MAX(CURRENTLY)
FROM Items);
