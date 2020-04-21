import datetime
import json
from pathlib import Path
import urllib
from typing import Iterable

import pandas as pd


def get_data(
    out_path: Path, states: Iterable[str], overwrite: bool = False
) -> pd.DataFrame:
    out_file = out_path / f"rki_infections_{datetime.datetime.now().date()}.pkl"
    if not out_file.is_file() or overwrite:
        dfs = []
        for state in states:
            dfs.append(fetch_infection_data_from_rki(bundesland=state))
            df = pd.concat(dfs, axis=0)
            df.to_pickle(out_file)
    else:
        df = pd.read_pickle(out_file)
    return df


def prepare_daily_infections(df: pd.DataFrame, n_cases: int) -> pd.DataFrame:
    return (
        df.groupby(["Bundesland", pd.Grouper(key="Meldedatum", freq="D")])[
            ["Neuinfektionen"]
        ]
        .sum()
        .sort_index()
        .reset_index()
        .assign(
            infections_cumulative=lambda df: df.groupby("Bundesland")[
                "Neuinfektionen"
            ].transform(lambda s: s.cumsum())
        )
        .pipe(add_days_since_n_cases, n_cases=n_cases)
    )


def add_measures(df: pd.DataFrame, measures: pd.DataFrame) -> pd.DataFrame:
    return (
        df.merge(
            measures.groupby(["Bundesland", "gueltig_ab"])
            .agg(Maßnahmen=("Maßnahme", lambda s: " und ".join(list(s))))
            .reset_index()
            .rename(columns={"gueltig_ab": "Meldedatum"}),
            on=["Bundesland", "Meldedatum"],
            how="left",
        )
        .sort_values(["Bundesland", "Meldedatum"])
        .reset_index(drop=True)
    )


def fetch_infection_data_from_rki(
    bundesland: str = "Bayern", offset: int = 0
) -> pd.DataFrame:
    """
    Fetch Covid-19-Cases from https://experience.arcgis.com/experience/478220a4c454480e823b17327b2bf1d4/page/page_0/
    
    Args:
        bundesland: written like displayed on the website, a string
    Returns:
        a Dataframe containing all historical infections data of a bundesland
    """

    url_endpoint = "https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/rest/services/RKI_COVID19/FeatureServer/0/query"
    params = {
        "f": "json",
        "where": f"Bundesland='{bundesland}'",
        "returnGeometry": "false",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "ObjectId,AnzahlFall,Meldedatum,Geschlecht,Altersgruppe",
        "orderByFields": "Meldedatum asc",
        "resultOffset": offset,
        "resultRecordCount": 2000,
        "cacheHint": "true",
    }

    url_query = f"{url_endpoint}?{urllib.parse.urlencode(params)}"

    with urllib.request.urlopen(url_query) as url:
        data = json.loads(url.read().decode())["features"]

    data_list = [
        (
            datetime.datetime.fromtimestamp(x["attributes"]["Meldedatum"] / 1e3),
            x["attributes"]["AnzahlFall"],
            x["attributes"]["Geschlecht"],
            x["attributes"]["Altersgruppe"],
            bundesland,
        )
        for x in data
    ]

    df = pd.DataFrame(
        data_list,
        columns=[
            "Meldedatum",
            "Neuinfektionen",
            "Geschlecht",
            "Altersgruppe",
            "Bundesland",
        ],
    )

    if len(data_list) >= 2000:
        df = df.append(fetch_infection_data_from_rki(bundesland, offset + 2000))

    return df


def add_days_since_n_cases(df: pd.DataFrame, n_cases: int) -> pd.DataFrame:
    return df.merge(
        df.query(f"infections_cumulative >= {n_cases}")
        .groupby("Bundesland")
        .agg(**{f"date_{n_cases}_cases": ("Meldedatum", "min")}),
        left_on="Bundesland",
        right_index=True,
    ).assign(
        **{
            f"days_since_{n_cases}_cases": lambda df: (
                df["Meldedatum"] - df[f"date_{n_cases}_cases"]
            ).dt.days
        }
    )


def read_measure_data(data_path) -> pd.DataFrame:
    return (
        pd.read_csv(
            data_path / "corona_measures - Measures_Overview.csv",
            parse_dates=["gueltig_ab", "gueltig_bis", "datum_publ"],
            infer_datetime_format=True,
        )
        .rename(columns={"bundesland": "bundesland_code"})
        .merge(
            pd.read_csv(
                data_path / "corona_measures - BL Resarch Mapping.csv",
                usecols=["bundesland", "short"],
            ).rename(columns={"short": "bundesland_code"}),
            on="bundesland_code",
        )
    )[
        [
            "bundesland",
            "category",
            "datum_publ",
            "gueltig_ab",
            "gueltig_bis",
            "beschreibung",
        ]
    ].rename(
        columns={"bundesland": "Bundesland", "category": "Maßnahme"}
    )
