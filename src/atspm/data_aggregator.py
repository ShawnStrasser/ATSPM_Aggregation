import os
from jinja2 import Environment, FileSystemLoader
from .data_saver import save_data


def render_query(query_name, **kwargs):
    print(query_name)
    # add from_table = 'raw_data' to the kwargs dictionary
    kwargs['from_table'] = 'raw_data'
    # Get the directory that contains the SQL templates
    template_dir = os.path.join(os.path.dirname(__file__), 'queries')
    # Create a Jinja2 environment with the FileSystemLoader
    env = Environment(loader=FileSystemLoader(template_dir))
    # Get the template by name
    template = env.get_template(f"{query_name}.sql")
    #print(template)
    # Render the template with the provided keyword arguments
    return template.render(**kwargs)

def aggregate_data(conn, aggregation_name, **kwargs):
    query = render_query(aggregation_name, **kwargs)
    #if aggregation_name == 'split_failures':
        #print(query)
    query = f"CREATE OR REPLACE TABLE {aggregation_name} AS {query}"
    conn.execute(query)
