# Silver Video Transforms

Planned AWS Glue ETL jobs for heavier video transformations.

This area will handle:

- Kaggle trending video CSV to Silver Parquet.
- YouTube API trending video JSON to Silver Parquet.
- Schema alignment between historical Kaggle data and live API data.
- Partitioned Silver outputs under `youtube/videos/source=.../region=...`.

The category/reference JSON transform stays in Lambda because it is small and
simple. Video transforms belong in Glue because they can grow larger and need
better support for backfills, partition management, and future Gold aggregation.
