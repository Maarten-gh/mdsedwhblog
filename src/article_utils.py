def visualzize_sql_code(code: str):
    from IPython.display import display_markdown
    display_markdown(f"```sql\n{code}\n```""", raw=True)

def visualzize_markdown_code(code: str):
    from IPython.display import display_markdown
    display_markdown(code, raw=True)