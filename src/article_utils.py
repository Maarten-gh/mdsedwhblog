# Next, import the necessary modules
import plotly.express as px
import plotly.io as pio
import plotly.graph_objects as go

def render_mappings(schema_mapping_metadata):
    node_names = [f'{t["source"]["schema"]}.{t["source"]["table"]}' for t in schema_mapping_metadata["table_mappings"]] + [f'{t["target"]["schema"]}.{t["target"]["table"]}' for t in schema_mapping_metadata["table_mappings"]]
    link_sources = [i for i in range(len(schema_mapping_metadata["table_mappings"]))]
    link_targets = [i for i in range(len(schema_mapping_metadata["table_mappings"]), 2 * len(schema_mapping_metadata["table_mappings"]))]
    link_values = [1 for i in range(len(schema_mapping_metadata["table_mappings"]))]

    data = dict(
        node = dict(
            pad = 15,
            thickness = 20,
            line = dict(
            color = "black",
                width = 0.5
            ),
            label = node_names,
        ),
        link = dict(
            source = link_sources,
            target = link_targets,
            value = link_values
        )
    )

    # Create the figure and plot the diagram
    fig = go.Figure(data = [go.Sankey(data)])
    fig.update_layout(title_text="Lineage", font_size=10)
    fig.show()