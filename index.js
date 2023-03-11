












async function load_data(topic, question, div_id){
    if (!(div_id in data[topic][question])){
        path = "assets/" + data[topic][question]["data_path_" + div_id]
        data[topic][question][div_id] = await fetch(path).then(response => {return response.json()})
        return data[topic][question][div_id]
    } else {
        return data[topic][question][div_id]
    }
}

function download(data_str, title, file_ending){
    const downloadLink = document.createElement('a');
    downloadLink.download = title + "." + file_ending;
    if (file_ending == "csv"){
        data_str = window.URL.createObjectURL(new Blob([data_str], { type: 'text/csv' }))
    } else if (file_ending == "json"){
        data_str = "data:text/json;charset=utf-8," + encodeURIComponent(JSON.stringify(data_str))
    }
    downloadLink.href = data_str
    downloadLink.style.display = 'none';
    document.body.appendChild(downloadLink);
    downloadLink.click();
    document.body.removeChild(downloadLink);
}


function add_plot(topic, question, div_id){
    div = add_div(div_id, "dataDiv")
    plot = data[topic][question][div_id]
    plot["config"]["staticPlot"] = (screen.width < 850)
    Plotly.newPlot(div, plot["traces"], plot["layout"], plot["config"])
}

async function save_plot(topic, question, div_id){
    await Plotly.toImage(document.getElementById(div_id), {format:'png',height:500,width:1000}).then(function(png_str){
        title = data[topic][question][div_id]["layout"]["title"]["text"].toLowerCase().replaceAll(" ", "_")
        download(png_str, title, "png")
    })
}

function save_plot_data(topic, question, div_id){
    x = data[topic][question][div_id]["traces"][0]["x"]
    y_list = []
    y_names = []
    for (trace of data[topic][question][div_id]["traces"]){
        y_list.push(trace["y"])
        y_names.push(trace["name"])
    }
    var rows = ["x," + y_names.join(",")]
    for (i=0; i<x.length; i++){
        row = [x[i]]
        for (j=0; j<y_list.length; j++){
            row.push(y_list[j][i])
        }
        rows.push(row.join(","))
    }
    title = data[topic][question][div_id]["layout"]["title"]["text"].toLowerCase().replaceAll(" ", "_")
    download(rows.join("\n"), title, "csv")
}


async function add_async_load_plot(topic, question, div_id){
    div = add_div(div_id, "dataDiv")
    plot = await load_data(topic, question, div_id)
    plot["config"]["staticPlot"] = (screen.width < 850)
    Plotly.newPlot(div, plot["traces"], plot["layout"], plot["config"])
}

async function save_async_load_plot(topic, question, div_id){
    save_plot(topic, question, div_id)
}

async function save_async_load_plot_data(topic, question, div_id){
    save_plot_data(topic, question, div_id)
}


function add_text(topic, question, div_id){
    div = add_div(div_id, "textDiv")
    div.insertAdjacentHTML('beforeend', data[topic][question][div_id]);
}


async function add_map(topic, question, div_id){
    div = add_div(div_id, "dataDiv")
    plot = await load_data(topic, question, div_id)
    plot["config"]["staticPlot"] = (screen.width < 850)
    Plotly.newPlot(div, plot["traces"], plot["layout"], plot["config"])
}

async function save_map(topic, question, div_id){
    await Plotly.toImage(document.getElementById(div_id), {format:'png',height:500,width:1000}).then(function(png_str){
        title = data[topic][question][div_id+"_data"]["layout"]["title"]["text"].toLowerCase().replaceAll(" ", "_") + "_map"
        download(png_str, title, "png")
    })
}

function save_map_data(topic, question, div_id){
    div_id += "_data"
    title = data[topic][question][div_id]["layout"]["title"]["text"].toLowerCase().replaceAll(" ", "_") + "_map"
    download(data[topic][question][div_id]["traces"], title, "json")
}


async function add_table(topic, question, div_id){
    div = add_div(div_id, "dataDiv")
    div.style.width = "100%"
    div.style.aspectRatio = "auto"
    table = await load_data(topic, question, div_id)
    div.innerHTML = get_table_html(table)
}

function get_table_html(table){
    let head = "<tr>"
    for (var i in table["head"]){
        if (i == 0){
            head += "<th>" + table["head"][i] + "</th>"
        } else {
            head += "<th class=\"sorting\" onclick=\"sort_table(event)\">" + table["head"][i] + "</th>"
        }
    }
    head += "</tr>"
    const body = table["body"].map(row => "<tr>" + row.map(v => "<td>" + v + "</td>").join("") + "</tr>").join("")
    return "<h3>" + table["title"] + "</h3><table style='margin: 0 auto;'>" + head + body + "</table>"
}

async function sort_table(e){
    for (var child=e.srcElement.parentElement.firstChild.nextSibling; child!==null; child=child.nextSibling) {
        if (child != e.srcElement && !child.classList.contains('sorting')){
            child.classList.remove('sorting_asc', 'sorting_desc');
            child.classList.add('sorting');
        }
    }

    if (e.srcElement.classList.contains('sorting')){
        e.srcElement.classList.remove('sorting');
        e.srcElement.classList.add('sorting_desc');
        var th = e.srcElement
        var ascending = false
    } else if (e.srcElement.classList.contains('sorting_desc')){
        e.srcElement.classList.remove('sorting_desc');
        e.srcElement.classList.add('sorting_asc');
        var th = e.srcElement
        var ascending = true
    } else {
        e.srcElement.classList.remove('sorting_asc');
        e.srcElement.classList.add('sorting');
        var th = e.srcElement.parentElement.firstChild
        var ascending = true
    }
    
    const getCellValue = (tr, idx) => tr.children[idx].innerText.replaceAll(",", "") || tr.children[idx].textContent.replaceAll(",", "");

    const comparer = (idx, asc) => (a, b) => ((v1, v2) => 
        v1 !== '' && v2 !== '' && !isNaN(v1) && !isNaN(v2) ? v1 - v2 : v1.toString().localeCompare(v2)
        )(getCellValue(asc ? a : b, idx), getCellValue(asc ? b : a, idx));

    const table = th.closest('table');
    Array.from(table.querySelectorAll('tr:nth-child(n+2)'))
        .sort(comparer(Array.from(th.parentNode.children).indexOf(th), ascending))
        .forEach(tr => table.appendChild(tr));
    
    for (var i in table.rows){
        if (i > 0){
            table.rows[i].cells[0].innerText = String(i)
        }
    }
}

function save_table_data(topic, question, div_id){
    div_id += "_data"
    title = data[topic][question][div_id]["title"].toLowerCase().replaceAll(" ", "_") + "_table"
    csv_str = data[topic][question][div_id]["head"].join(',') + "\n"
    
    for (row of data[topic][question][div_id]["body"]){
        csv_str += row.join(',') + "\n"
    }
    download(csv_str, title, "csv")
}