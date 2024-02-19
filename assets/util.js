async function show_plot(plot){
    div = document.getElementById(plot["filename"])
    Plotly.newPlot(
        div,
        plot["traces"],
        plot["layout"],
        {staticPlot: screen.width < 850, responsive: true}
    );
}

async function init(){
    const plots = await fetch("plots.json").then((response) => {
        return response.json();
    });
    for (const [_, plot] of Object.entries(plots)) {
        show_plot(plot);
    }
}