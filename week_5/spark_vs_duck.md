# Pyspark
** Disadvantage **
- Difficult to Setup/Install 
- Difficult to use with JupiterNotebook, since there might be problems with updating Kernel in JPN
- Manny Depprecated APIs (ex toPandas()) that require you to manually install other dependencies
- Sometimes running into memory limitation issues due to conversion to Data Frame (Solvable with partitioning)
- Seemingly slower than DuckDB, but ther eneeds to be performed some more tests. Mostlikely due to partitioning itself


** Advantages **
- ORM like querries (more programmatic)
- Can be paralelized and horizontally scaled


# DuckDB 
** Disadvantage **
- Requires to know SQL
- RAM limited, but very highly optimized


** Advantages **
- Easy to setup and run
- Fast and Memory efficient but hard to compare. (I did not feel issues with DusckDB ulike with Pyspark)