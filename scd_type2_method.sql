
/* Prerequisites for this method

- There should only be one row for each naturalKey in the source table

*/

MERGE INTO destination dst USING (

/* UPDATES TO EXISTING ROWS and NEW INSERTS

The logic behind the first statement is to find all the rows in the source table. 
All the rows found here will either UPDATE existing rows in the destination table, using the mergeKey, or insert new rows for new mergeKeys.
We set the mergeKey to be the naturalKey of the table. This makes sure, that IF we match on the mergeKey in our MERGE statement, we update the row in the destination table. Whenever we UPDATE rows in the destination table
we only update the Meta_validTo, because we are "closing" this version of the row.

If the mergeKey does not match anything in the destination table, it will automatically be placed in the WHEN NOT MATCHED part of the MERGE statement and be INSERTED.

This way we ensure, that any rows that MATCH in our destination will be caught and we can use that to update the Meta_validTo and the rows that does not match from our source will be INSERTED.

*/

SELECT
	src.naturalKey AS mergeKey,
	src.*,
	HASHBYTES(
		'SHA2_256', 
		UPPER(src.attributeA)
	) AS Meta_columnHash
FROM
	[source] AS src

UNION

/* INSERTS FOR EXISTING naturalKeys

When we find a match for the naturalKey, we need to INSERT a new row for the new values for that key. 
The logic behind this second statement lies within the WHERE clause. It basically says "Find me the latest rows that match on the naturalKey in our destination table, and where we have a difference in the non-key attributes".
If this WHERE logic returns any rows, we know that we have a row in the destination that matches one in our source AND there is a difference in the non-key columns whereby we need to INSERT a new "version" of that row.
Because we do not want to update the existing row with our new non-key value, we set the mergeKey to NULL - this will make sure, that the MERGE statement does not MATCH on the key for this row and we then INSERT the row instead.

*/

SELECT 
	NULL as mergeKey,
	src.*,
	dst.Meta_columnHash
FROM 
	[source] AS src
INNER JOIN destination AS dst ON src.naturalKey = dst.naturalKey
WHERE dst.Meta_validTo = '9999-12-31' 
AND   dst.Meta_columnHash <>
	HASHBYTES(
		'SHA2_256',
		UPPER(src.attributeA)
	)

/* Result of the 'src' statement.

Example: The naturalKey 1 is already in the destination table.

mergeKey | naturalKey | attribute | Meta_columnHash
1		   1            valueB      0x123123        
NULL	   1            valueB      0x123123

Notice that because we have a match in the second query of the 'merge_src' query, we get the NULL mergeKey. And because we take the values from the 'src' in the second part, the 'attribute' is the same for both.
The NULL mergeKey will be inserted witht the new value(s) in attribute and the mergeKey = 1 will have it's Meta_validTo updated because there will be a MATCH in the MERGE statement.

Example: The naturalKey does not exists in the destionation

mergeKey | naturalKey | attribute | Meta_columnHash
1		   1            valueB      0x123123

The MERGE statement will not MATCH on the mergeKey, why this row will just be INSERTED.      

*/

) as merge_src
ON merge_src.mergeKey = dst.naturalKey
WHEN MATCHED AND dst.Meta_validTo = '9999-12-31' AND merge_src.Meta_columnHash <> dst.Meta_columnHash
THEN UPDATE 
	SET dst.Meta_validTo = GETDATE()
WHEN NOT MATCHED THEN
	INSERT (    naturalKey,     attributeA,     Meta_columnHash, Meta_validFrom, Meta_validTo)
	VALUES (merge_src.naturalKey, merge_src.attributeA, merge_src.Meta_columnHash, GETDATE(),		 '9999-12-31');