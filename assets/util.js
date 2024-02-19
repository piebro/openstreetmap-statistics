function show_plot(plot){
    div = document.getElementById(plot["filename"])
    if (plot["type"] == "plotly_plot"){
        Plotly.newPlot(
            div,
            plot["traces"],
            plot["layout"],
            {staticPlot: screen.width < 850, responsive: true}
        );
    } else if (plot["type"] == "table"){
        div.innerHTML='<div class="table-container">' + plot["innerHTML"] + '</div>'
    } else {
        console.error("unknown type: " + plot["type"])
    }
    
}

async function init(){
    const plots = await fetch("plots.json").then((response) => {
        return response.json();
    });
    for (const [_, plot] of Object.entries(plots)) {
        show_plot(plot);
    }

    
    // TODO: only make the first two columns sticky if the first column is "rank".
    // document.addEventListener('DOMContentLoaded', function() {
    // Calculate and set offset for each first thead th in rows
    var firstTheadThWidth = document.querySelector('thead th').offsetWidth;
    var secondTheadThs = document.querySelectorAll('thead th:nth-child(2)');
    secondTheadThs.forEach(function(th) {
        th.style.left = firstTheadThWidth + 'px';
    });

    // Calculate and set offset for each first tbody th in rows
    document.querySelectorAll('tbody tr').forEach(function(row) {
        var firstThWidth = row.querySelector('th').offsetWidth;
        var secondTh = row.querySelector('th:nth-child(2)');
        if (secondTh) {
            secondTh.style.left = firstThWidth + 'px';
        }
    });
    // });
}

