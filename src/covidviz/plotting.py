import altair as alt
import pandas as pd


def combine_summary_plots(
    df, x_var, x_title, y_var, y_title, y_format, max_activity, width=200, height=125,
):

    combined_plots = []
    sub_combined = []
    for state in df["Bundesland"].unique():
        state_plot = plot_infection_activity_summary(
            df,
            state=state,
            title=state,
            x_var=x_var,
            x_title=x_title,
            y_var=y_var,
            y_title=y_title,
            y_format=y_format,
            max_activity=max_activity,
            width=width,
            height=height,
        )
        if len(sub_combined) == 4:
            combined_plots.append(alt.hconcat(*sub_combined))
            sub_combined = []
        elif len(sub_combined) != 0:
            state_plot.layer[1].encoding.y.axis.title = " "
        else:
            pass
        sub_combined.append(state_plot)
    combined_plots.append(alt.hconcat(*sub_combined))
    return alt.vconcat(*combined_plots).configure_axis(
        labelFontSize=8, titleFontSize=10
    )


def plot_infection_activity_summary(
    df,
    state,
    title,
    x_var,
    x_title,
    y_var,
    y_title,
    y_format,
    max_activity,
    width=250,
    height=150,
):
    min_date = df.query(f"Bundesland == '{state}'")["Meldedatum"].min()
    max_date = df.query(f"Bundesland == '{state}'")["Meldedatum"].max()
    max_y = df[y_var].max()
    date_range = [
        str(time.date())
        for time in pd.date_range(start=min_date, freq="W", end=max_date)
    ]
    base = alt.Chart(df.query(f"Bundesland == '{state}'"), title=title,).encode(
        x=alt.X(
            x_var,
            axis=alt.Axis(
                title=x_title, offset=0, grid=False, values=date_range, format="%b %d"
            ),
        ),
        y=alt.Y(f"{y_var}:Q", axis=alt.Axis(format=y_format)),
    )
    points = base.mark_point(color="DarkSlateBlue").encode(y=alt.Y(f"{y_var}:Q"),)
    lines = (
        points.transform_loess(
            on=x_var, loess=y_var, as_=[x_var, f"{y_var}_loess"], groupby=["Bundesland"]
        )
        .mark_line(color="DarkSlateBlue")
        .encode(
            y=alt.Y(
                f"{y_var}_loess:Q",
                axis=alt.Axis(format=y_format, title=y_title),
                scale=alt.Scale(domain=(0, 1)),
            ),
        )
    )

    total_activity = base.mark_area(color="#5ba3cf").encode(
        y=alt.Y(
            "total_activity:Q",
            axis=alt.Axis(orient="right", labels=False, ticks=False, title=""),
            scale=alt.Scale(domain=(-max_activity, max_activity)),
        ),
        opacity=alt.value(0.2),
    )
    infections_activity_summary = (
        (total_activity + lines)
        .resolve_scale(y="independent")
        .properties(width=width, height=height)
    )
    return infections_activity_summary


def plot_infection_details(
    df,
    state,
    title,
    x_var,
    x_title,
    y_var,
    y_title,
    y_format,
    max_activity,
    width=900,
    height=350,
):
    min_date = df.query(f"Bundesland == '{state}'")["Meldedatum"].min()
    max_date = df.query(f"Bundesland == '{state}'")["Meldedatum"].max()
    date_range = [
        str(time.date())
        for time in pd.date_range(start=min_date, freq="W", end=max_date)
    ]
    base = alt.Chart(df.query(f"Bundesland == '{state}'"), title=title,).encode(
        x=alt.X(
            x_var,
            axis=alt.Axis(
                title=x_title, offset=0, grid=False, values=date_range, format="%b %d"
            ),
        ),
        y=alt.Y(f"{y_var}:Q", axis=alt.Axis(format=y_format)),
    )
    points = base.mark_point(color="DarkSlateBlue").encode(
        y=alt.Y(f"{y_var}:Q"),
        tooltip=list(
            set(["Meldedatum", x_var, "Neuinfektionen", "infections_cumulative"])
        ),
    )
    lines = (
        points.transform_loess(
            on=x_var, loess=y_var, as_=[x_var, f"{y_var}_loess"], groupby=["Bundesland"]
        )
        .mark_line(color="DarkSlateBlue")
        .encode(
            y=alt.Y(f"{y_var}_loess:Q", axis=alt.Axis(format=y_format, title=y_title))
        )
    )

    total_activity = base.mark_area(color="#5ba3cf").encode(
        y=alt.Y(
            "total_activity:Q",
            axis=alt.Axis(format="%", orient="right", title="Google Mobility Index"),
            scale=alt.Scale(domain=(-max_activity, max_activity)),
        ),
        opacity=alt.value(0.2),
    )
    measures = (
        base.mark_point(size=400, shape="diamond", color="#125ca4", fill="#125ca4")
        .transform_calculate(y_level="0")
        .encode(
            y=alt.Y("y_level:Q", axis=alt.Axis(orient="right")),
            tooltip=["Meldedatum", "Maßnahmen"],
        )
        .transform_filter("datum.num_measures > 0")
    )
    infection_details = (
        ((total_activity + measures) + (points + lines))
        .resolve_scale(y="independent")
        .properties(width=width, height=height)
    )
    infection_details.layer[1].encoding.y.title = ""
    return infection_details


def plot_activity_details(
    df, state, title, x_var, x_title, activity_cols, max_activity, width=900, height=350
):
    min_date = df.query(f"Bundesland == '{state}'")["Meldedatum"].min()
    max_date = df.query(f"Bundesland == '{state}'")["Meldedatum"].max()
    date_range = [
        str(time.date())
        for time in pd.date_range(start=min_date, freq="W", end=max_date)
    ]
    selection = alt.selection_multi(fields=["Mobility Category"])
    color = alt.condition(
        selection,
        alt.Color("Mobility Category:N", scale=alt.Scale(scheme="blues"), legend=None),
        alt.value("lightgray"),
    )

    activity_base = (
        alt.Chart(df.query(f"Bundesland == '{state}'"), title=title,)
        .transform_fold(
            fold=activity_cols, as_=["Mobility Category", "mobility_change_percent"]
        )
        .transform_calculate(
            as_="Google Mobility Index", calculate="datum.mobility_change_percent / 100"
        )
    )

    activity = activity_base.mark_area().encode(
        x=alt.X(
            x_var,
            axis=alt.Axis(
                title=x_title, offset=0, grid=False, values=date_range, format="%b %d"
            ),
        ),
        y=alt.Y(
            "Google Mobility Index:Q",
            axis=alt.Axis(format="%", orient="left"),
            scale=alt.Scale(domain=(-max_activity, max_activity)),
        ),
        color=color,
        opacity=alt.value(0.8),
    )

    legend = (
        activity_base.mark_point()
        .encode(
            y=alt.Y(
                "Mobility Category:N",
                axis=alt.Axis(orient="right", grid=False, ticks=False, offset=2),
                title="Click to select",
            ),
            color=color,
        )
        .add_selection(selection)
    )
    legend.title = "Category"
    return (
        activity.properties(width=width - 30, height=height)
        | legend.properties(width=30, height=height)
    ).configure_axis(grid=True)
