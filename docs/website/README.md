# Build the website for Doris documentations

## Prerequisites

1. Install PiPy (If not installed)

    ```
    curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
    python get-pip.py
    ```

2. Install sphinx and other dependencies

    ```
    pip install sphinx                      # Sphinx main program
    pip install recommonmark                # Sphinx markdown extension
    pip install sphinx-markdown-tables      # Sphinx markdown table render extension
    pip install jieba                       # Sphinx Chinese tokenizer
    pip install sphinx_rtd_theme            # Sphinx Read-the-Docs theme
    ```

## Build the website

```
sh build_site.sh 
```

## Start web server

```
cd build/html/
nohup python -m SimpleHTTPServer &
```

You can start any web server you like.

## Browse website

```
http://localhost:8000/
```
