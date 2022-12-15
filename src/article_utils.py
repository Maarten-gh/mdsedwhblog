"""
This file contains functionality that is used in articles but not directly contributes to the topics of the articles.
"""

def visualzize_sql_code(code: str):
    """
    Renders SQL code with syntax highlighting in a Jupyter notebook.

    Parameters
    ----------
    - code
      The SQL code to render.
    """
    from IPython.display import display_markdown
    display_markdown(f"```sql\n{code}\n```""", raw=True)

def visualzize_markdown_code(code: str):
    """
    Renders Markdown code in a Jupyter notebook.

    Parameters
    ----------
    - code
      The Markdown code to render.
    """
    from IPython.display import display_markdown
    display_markdown(code, raw=True)