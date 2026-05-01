use polars::prelude::*;

/// Produces the reconciliation dataframe from the details [df]
pub fn gather_df_reconciliation(df_det: &DataFrame) -> PolarsResult<DataFrame> {
    df_det
        .clone()
        .lazy()
        .select([
            col("Date").alias("Datum"),
            col("Cafe_Cash").alias("Cafe Cash"),
            col("Cafe_Card").alias("Cafe Card"),
            col("Cafe_Tips").alias("Cafe Tips"),
            col("Culture_Cash").alias("Culture Cash"),
            col("Culture_Card").alias("Culture Card"),
            col("Culture_Tips").alias("Culture Tips"),
            col("Culture (PaidOut) Total").alias("PaidOut"),
        ])
        .sort(["Datum"], SortMultipleOptions::new().with_nulls_last(true))
        .collect()
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use crate::test_fixtures::{details_df_03, reconciliation_df_03};
    use crate::test_utils::assert_dataframe;

    use super::*;

    #[rstest]
    fn test_gather_df_reconciliation(details_df_03: DataFrame, reconciliation_df_03: DataFrame) {
        let out = gather_df_reconciliation(&details_df_03)
            .expect("should be able to collect reconciliation_df");
        assert_dataframe(&out, &reconciliation_df_03);
    }
}
