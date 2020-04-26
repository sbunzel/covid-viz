from pathlib import Path
import streamlit as st

import covidviz.data as data
import covidviz.plotting as plotting

md_dir = Path(__file__).parent / "markdowns"
data_dir = Path(__file__).parent.parent / "data"


def main():

    language = st.sidebar.radio(
        "Select display language", options=["english", "deutsch"], format_func=str.title
    )
    with open(md_dir / language / "intro.md", mode="r") as f:
        intro = f.read()
    st.markdown(intro)
    plot_data = data.PlotData(out_path=data_dir)
    df = plot_data.df
    state_mapper = (
        {v: k for k, v in data.STATE_MAPPER.items()}
        if language == "english"
        else {v: v for k, v in data.STATE_MAPPER.items()}
    )
    max_activity = (
        df[["total_neg_activity", "total_pos_activity"]].abs().max().max() // 50 + 1
    ) * 0.5
    summary_plot = plotting.combine_summary_plots(
        df=df,
        x_var="Meldedatum",
        x_title="",
        y_var="absolute_growth",
        y_title="Absolute Growth in Cumulative Cases",
        max_activity=max_activity,
    )
    st.altair_chart(summary_plot, use_container_width=False)
    state = st.selectbox(
        "Select a state to see the details",
        options=list(state_mapper.keys()),
        format_func=state_mapper.get,
    )
    infection_title = f"{state_mapper[state]}: Infections (last updated: {plot_data.infections_last_updated[state].date()})"
    infection_plot = plotting.plot_infection_details(
        df=df,
        state=state,
        title=infection_title,
        x_var="Meldedatum",
        x_title="Date",
        y_var="absolute_growth",
        y_title="Absolute Growth in Cumulative Cases",
        max_activity=max_activity,
    )
    st.altair_chart(infection_plot, use_container_width=False)
    activity_title = f"{state_mapper[state]}: Detailed Mobility Report (last updated: {plot_data.mobility_last_updated[state].date()})"
    activity_plot = plotting.plot_activity_details(
        df=df,
        state=state,
        title=activity_title,
        x_var="Meldedatum",
        x_title="Date",
        activity_cols=plot_data.activity_cols,
        max_activity=max_activity,
        width=830,
    )
    st.altair_chart(activity_plot, use_container_width=False)


if __name__ == "__main__":
    main()
