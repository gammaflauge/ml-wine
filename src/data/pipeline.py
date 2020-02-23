""" main data pipeline for transforming the wine dataset """
from pathlib import Path
from metaflow import FlowSpec, step, IncludeFile

PROJECT_DIR = Path(__file__).resolve().parents[2]


class WineFlow(FlowSpec):
    """
    A flow to read in the wine dataset -- see src.data.download_raw.py
    https://archive.ics.uci.edu/ml/datasets/Wine
    """

    raw_data_path = PROJECT_DIR / "data" / "raw" / "wine.csv"
    raw_wine_data = IncludeFile(
        "wine_data",
        help="These data are the results of a chemical analysis of wines grown in the same region in Italy but derived from three different cultivars. The analysis determined the quantities of 13 constituents found in each of the three types of wines.",
        default=raw_data_path,
    )

    @step
    def start(self):
        """
        Read data/raw/wine.csv into a pandas df
        """
        import pandas as pd
        from io import StringIO

        self.dataframe = pd.read_csv(StringIO(self.raw_wine_data))
        self.next(self.rename_cols)

    @step
    def rename_cols(self):
        """
        Give the wine data some computer friendlier column names
        Save to "data/interim/wine_df_nice_cols.csv"
        """
        # could not find out what this means! All google hits were for this dataset
        self.dataframe = self.dataframe.rename(
            {"OD280/OD315 of diluted wines": "od280_over_od315"}, axis="columns"
        )
        self.dataframe.columns = [
            col.lower().replace(" ", "_") for col in self.dataframe.columns
        ]

        save_path = PROJECT_DIR / "data" / "interim" / "wine_df_nice_cols.csv"
        self.dataframe.to_csv(save_path, index=False)

        self.next(self.make_tidy)
    
    @step
    def make_tidy(self):
        """
        Create a tidy version of the wine dataframe
        Save to "data/interim/wine_df_tidy.csv"
        """
        self.dataframe.index.name = 'wine_id'
        self.dataframe = self.dataframe.reset_index()
        self.dataframe = self.dataframe.melt(id_vars=['wine_id', 'class'])

        save_path = PROJECT_DIR / "data" / "interim" / "wine_df_tidy.csv"
        self.dataframe.to_csv(save_path, index=False)

        self.next(self.end)

    @step
    def end(self):
        """
        End the flow.
        """
        pass


if __name__ == "__main__":
    WineFlow()
