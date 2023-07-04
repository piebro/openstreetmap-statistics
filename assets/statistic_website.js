data_cache = {}
data_cache_list = []
max_cache_size = 30

function options_to_selection(select, keys){
    prev_value = select.value
    while (select.firstChild) {
        select.removeChild(select.lastChild)
    }
    for (key of keys){
        var opt = document.createElement('option')
        opt.value = key
        opt.innerHTML = key.replaceAll("_", " ")
        select.appendChild(opt)
    }
    if (keys.includes(prev_value)){
        select.value = prev_value
    }
}

async function load_data(data_name){
    if (data_name in data_cache){
        return data_cache[data_name]
    } else {
        data_cache[data_name] = await fetch("assets/data/" + data_name + ".json").then(response => {return response.json()})
        data_cache_list.push(data_name)
        if (data_cache_list.length > max_cache_size){
            delete data_cache[data_cache_list.shift()]
        }
        return data_cache[data_name]
    }
}

function add_div(id, class_str, parent_div=null){
    let div = document.createElement('div')
    div.id = String(id)
    div.style.display = "inline-block"
    div.classList.add(class_str);
    if (parent_div == null){
        document.getElementById('data').appendChild(div)
    } else {
        parent_div.appendChild(div)
    }
    return div
}

function add_text(text){
    show_content = async function(div_id){
        div = add_div(div_id, "textDiv")
        div.insertAdjacentHTML('beforeend', text);
    }
    return {"show": show_content}
}

function get_plot_config(){
    return {"displayModeBar": false, "staticPlot":(screen.width < 850)}
}

function get_plot_layout(){
    return {
        "font": {"family": "Times", "size": "15"},
        "paper_bgcolor": "#dfdfdf",
        "plot_bgcolor": "#dfdfdf",
    }
}

function get_line_plot_layout(plot_title, unit, percent, bar_chart_on_top_of_each_other){
    layout = Object.assign({}, get_plot_layout(), {
        "margin": {"l": 55, "r": 55, "b": 55, "t": 55},
        "title": {"text": plot_title},
        "xaxis": {"title": {"text": "time"}},
        "yaxis": {"title": {"text": unit}, "rangemode": "tozero"},
    })
    if (percent){
        layout["yaxis"]["range"] = [0, 101]
    }
    if (bar_chart_on_top_of_each_other){
        layout["barmode"] = "stack"
    }
    return  layout
}

function get_map_plot_layout(plot_title, max_z_value){
    colorscale = [
        [0, "rgba(255,255,255,0)"],
        [0.00000001, "rgb(12,51,131)"],
        [1 / 1000, "rgb(10,136,186)"],
        [1 / 100, "rgb(242,211,56)"],
        [1 / 10, "rgb(242,143,56)"],
        [1, "rgb(217,30,30)"],
    ]
    layout = Object.assign({}, get_plot_layout(), {
        "margin": {"l": 20, "r": 20, "b": 35, "t": 35},
        "images": [{
            "source": "assets/background_map.png",
            "xref": "x",
            "yref": "y",
            "x": 0,
            "y": 180,
            "sizex": 360,
            "sizey": 180,
            "sizing": "stretch",
            "opacity": 1,
            "layer": "below",
        }],
        "xaxis": {"showgrid": false, "visible": false},
        "yaxis": {"showgrid": false, "visible": false, "scaleanchor": "x", "scaleratio": 1},
        "coloraxis": {"colorscale": colorscale, "cmin": 0, "cmax": max_z_value},
        "title": {"text": plot_title},
    })
    return layout
}

function add_single_line_plot(data_name, plot_title, unit, percent=false){
    show_content = async function(div_id){
        data = await load_data(data_name)
        div = add_div(div_id, "dataDiv")
        traces = [{"x": data["x"], "y": data["y"], "mode": "lines", "name": "", "hovertemplate": "%{x}<br>%{y:,} " + unit}]
        Plotly.newPlot(div, traces, get_line_plot_layout(plot_title, unit, percent), get_plot_config())
    }
    return {"show": show_content, "save_plot": async function(div_id){save_plot(data_name, div_id)}, "save_data": async function(){save_data(data_name)}}
}

function add_multi_line_plot(data_name, plot_title, unit, percent=false, on_top_of_each_other=false, bar_chart=false, reverse_y_names=false){
    show_content = async function(div_id){
        data = await load_data(data_name)
        div = add_div(div_id, "dataDiv")
        traces = []
        for (i in data["y_names"]){
            if (reverse_y_names){
                i = data["y_names"].length - 1 - i    
            }
            trace = {
                "x": data["x"],
                "y": data["y_list"][i],
                "mode": "lines",
                "name": data["y_names"][i],
                "hovertemplate": "%{x}<br>%{y:,} " + unit
            }
            if (on_top_of_each_other){
                trace["stackgroup"] = "one"
            }
            if (bar_chart){
                trace["type"] = "bar"
            }
            traces.push(trace)
        }
        
        bar_chart_on_top_of_each_other = bar_chart && on_top_of_each_other
        Plotly.newPlot(div, traces, get_line_plot_layout(plot_title, unit, percent, bar_chart_on_top_of_each_other), get_plot_config())
    }
    return {"show": show_content, "save_plot": async function(div_id){save_plot(data_name, div_id)}, "save_data": async function(){save_data(data_name)}}
}

function get_map_plot_traces(x, y, z, max_z_value){
    traces = [{
            "type": "histogram2d",
            "x": x,
            "y": y,
            "z": z,
            "zmax": max_z_value,
            "histfunc": "sum",
            "autobinx": false,
            "xbins": {"start": 0, "end": 360, "size": 1},
            "autobiny": false,
            "ybins": {"start": 0, "end": 180, "size": 1},
            "coloraxis": "coloraxis",
    }]
    return traces
}

function add_map_plot(data_name, plot_title){
    show_content = async function(div_id){
        data = await load_data(data_name)
        div = add_div(div_id, "dataDiv")
        traces = get_map_plot_traces(data[data_name]["x"], data[data_name]["y"], data[data_name]["z"], data["max_z_value"])
        Plotly.newPlot(div, traces, get_map_plot_layout(plot_title, data["max_z_value"]), get_plot_config())
    }
    return {"show": show_content, "save_plot": async function(div_id){save_plot(data_name, div_id)}, "save_data": async function(){save_data(data_name)}}
}

function add_multiple_map_plots(data_name){
    show_content = async function(div_id){
        data = await load_data(data_name)
        for (const map_name of Object.keys(data[data_name])) {
            div = add_div(map_name, "dataDiv")
            traces = get_map_plot_traces(data[data_name][map_name]["x"], data[data_name][map_name]["y"], data[data_name][map_name]["z"], data["max_z_value"])
            Plotly.newPlot(div, traces, get_map_plot_layout(map_name, data["max_z_value"]), get_plot_config())
        }
    }
    save_map_plots = async function(div_id){
        data = await load_data(data_name)
        for (const map_name of Object.keys(data[data_name])){
            save_plot(data_name + "_" + map_name.replaceAll(" ", "_"), map_name)
        }
    }
    return {"show": show_content, "save_plot": save_map_plots, "save_data": async function(){save_data(data_name)}}
}

function add_table(data_name, title, y_names_category, data_name_total, data_name_name_to_link=undefined){
    show_content = async function(div_id){
        data = await load_data(data_name)
        data_total = await load_data(data_name_total)
        if (data_name_name_to_link !== undefined){
            name_to_link = await load_data(data_name_name_to_link)
        } else {
            name_to_link = {}
        }
        
        div = add_div(div_id, "dataDiv")
        div.style.width = "100%"
        div.style.aspectRatio = "auto"
        div.innerHTML = get_table_html(title, data, data_total, y_names_category, name_to_link)
    }
    return {"show": show_content, "save_data": async function(){save_data(data_name, data_name_total)}}
}

function get_table_html(title, data, data_total, y_names_category, name_to_link){
    let head_entries = ["Rank", y_names_category].concat(data["x"]).concat(["total"])
    let head = "<tr>"
    for (var i in head_entries){
        if (i == 0){
            head += "<th>" + head_entries[i] + "</th>"
        } else {
            head += "<th class=\"sorting\" onclick=\"sort_table(event)\">" + head_entries[i] + "</th>"
        }
    }
    head += "</tr>"
    let body = ""
    for (row_index in data["y_names"]){
        let name = data["y_names"][row_index]
        if (name in name_to_link){
            name = "<a href=" + name_to_link[name] + ">" + name + "</a>"
        }
        body += "<tr><td>" + String(parseInt(row_index) + 1) + "</td><td>" + name + "</td>"
        for (y_value of data["y_list"][row_index]){
            body += "<td>" + y_value.toLocaleString("en-US") + "</td>"
        }
        body += "<td>" + data_total["y_list"][row_index].toLocaleString("en-US") + "</td></tr>"
    }
    return "<h3>" + title + "</h3><table style='margin: 0 auto;'>" + head + body + "</table>"
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

async function save_plot(data_name, div_id){
    await Plotly.toImage(document.getElementById(div_id), {format:'png',height:500,width:1000}).then(function(png_str){
        download(png_str, data_name, "png")
    })
}


async function save_data(data_name, data_name_2=null){
    data = await load_data(data_name)
    download(data, data_name, "json")
    if (data_name_2 !== null){
        data_2 = await load_data(data_name_2)
        download(data_2, data_name_2, "json")
    }
}


function init(topics){
    selection_options = {}
    url_hash_to_topic_question = {}
    for (const [topic, topic_obj] of Object.entries(topics)) {
        selection_options[topic] = {}
        for (const [question, question_obj] of Object.entries(topic_obj)) {
            selection_options[topic][question] = {}
            url_hash_to_topic_question[question_obj["url_hash"]] = [topic, question]
        }
    }

    window.plausible = window.plausible || function() { (window.plausible.q = window.plausible.q || []).push(arguments) }

    
    update_select_0()
    if(window.location.hash) {
        url_hash = window.location.hash.substring(1)
        if (url_hash in url_hash_to_topic_question){
            topic = url_hash_to_topic_question[url_hash][0]
            question = url_hash_to_topic_question[url_hash][1]
            if (topic in selection_options && question in selection_options[topic]){
                document.getElementById('select_0').value = topic
                update_select_1()
                document.getElementById('select_1').value = question
            }
        }
    }
    update_content(topics, true)
    window.addEventListener('resize', function(event) {update_content(topics, false)}, true);
}

function update_select_0(){
    options_to_selection(document.getElementById('select_0'), Object.keys(selection_options))
    update_select_1()
}

function update_select_1(){
    current_select_0 = document.getElementById('select_0').value
    options_to_selection(document.getElementById('select_1'), Object.keys(selection_options[current_select_0]))
}

function on_change_select_0(topics){
    update_select_1()
    update_content(topics, true)
}

function on_change_select_1(topics){
    update_content(topics, true)
}

function get_current_select(){
    return [document.getElementById('select_0').value, document.getElementById('select_1').value]
}

async function update_content(topics, log_update){
    document.getElementById("data").innerHTML = ""  
    topic_question = get_current_select()
    topic = topic_question[0]
    question = topic_question[1]
    console.log(topic, question)
    if (log_update){
        plausible("Selected Plot(s)", {props: {"Topic Question": topic + ": " + question}})
    }

    for (const [index, content_functions] of topics[topic][question]["content_functions"].entries()){
        await content_functions["show"](index)
    }

    document.getElementById('save_plot_btn').onclick = async function(){
        for (const [index, content_functions] of topics[topic][question]["content_functions"].entries()){
            if ("save_plot" in content_functions){
                content_functions["save_plot"](index)
            }
        }
        plausible("Download Plot(s)", {props: {"Topic Question": topic + ": " + question}})
    }
    document.getElementById('save_data_btn').onclick = async function(){
        for (const [index, content_functions] of topics[topic][question]["content_functions"].entries()){
            if ("save_data" in content_functions){
                content_functions["save_data"](index)
            }
        }
        plausible("Download Data", {props: {"Topic Question": topic + ": " + question}})
    }
    history.replaceState(null, "", "#" + topics[topic][question]["url_hash"])
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