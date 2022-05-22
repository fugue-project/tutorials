author = "The Fugue Development Team"
comments_config = {"hypothesis": False, "utterances": False}
copyright = "2022"
exclude_patterns = ["**.ipynb_checkpoints", ".DS_Store", "Thumbs.db", "_build"]
execution_allow_errors = False
execution_excludepatterns = []
execution_in_temp = False
execution_timeout = 300
extensions = [
    "sphinx_togglebutton",
    "sphinx_copybutton",
    "myst_nb",
    "jupyter_book",
    "sphinx_thebe",
    "sphinx_comments",
    "sphinx_external_toc",
    "sphinx.ext.intersphinx",
    "sphinx_panels",
    "sphinx_book_theme",
    "sphinx_jupyterbook_latex",
    "sphinx.ext.autosectionlabel",
]
autosectionlabel_prefix_document = True
external_toc_exclude_missing = False
external_toc_path = "_toc.yml"
html_baseurl = ""
html_favicon = "docs/_static/fugue_logo_trimmed.svg"
html_logo = "images/logo_blue.svg"
html_sourcelink_suffix = ""
html_theme = "sphinx_book_theme"
html_theme_options = {
    "search_bar_text": "Search this book...",
    "launch_buttons": {
        "notebook_interface": "classic",
        "binderhub_url": "https://mybinder.org",
        "jupyterhub_url": "",
        "thebe": True,
        "colab_url": "",
    },
    "path_to_docs": "",
    "repository_url": "https://github.com/fugue-project/tutorials/",
    "repository_branch": "master",
    "google_analytics_id": "",
    "extra_navbar": 'Powered by <a href="https://jupyterbook.org">Jupyter Book</a>',
    "extra_footer": "",
    "home_page_in_toc": True,
    "use_repository_button": True,
    "use_edit_page_button": True,
    "use_issues_button": True,
}
html_title = "Fugue Tutorials"
jupyter_cache = ""
jupyter_execute_notebooks = "off"
language = None
latex_engine = "pdflatex"
myst_enable_extensions = [
    "colon_fence",
    "dollarmath",
    "linkify",
    "substitution",
    "tasklist",
]
myst_url_schemes = ["mailto", "http", "https"]
nb_output_stderr = "show"
numfig = True
panels_add_bootstrap_css = False
pygments_style = "sphinx"
suppress_warnings = ["myst.domains"]
use_jupyterbook_latex = True
use_multitoc_numbering = True
