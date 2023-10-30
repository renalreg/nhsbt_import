<br />
<div align="center">
  <a href="https://github.com/renalreg/nhsbt_import">
    <img src="" alt="Logo" width="80" height="80">
  </a>

  <h1 align="center">NHSBT Import</h1>

  <h3 align="center">
    A script for importing NHSBT data and creating an audit file
    <br/>
    <br/>
    
</div>


<div align="center">

<a>[![][issues-shield]][issues-url]</a>

</div>

## Getting Started
You will need to create a directory on the shared data drive, under NHSBT,  typically labelled using the date on which you commenced the process 

## Prerequisites
You will need python 3.11 and poetry installed. 

## Installation
Clone the repo

```sh
git clone https://github.com/renalreg/nhsbt_import.git
```
Setup the environment

```sh
poetry install
```

## Usage
Run the following, pointing at the directory you created

```sh
poetry run import.py -d /path/to/the/directory
```

This command will create an audit and error file in the declared directory. The error file holds any errors encountered during the run to aid with debugging.The audit file will be an excel sheet that tracks new and updated data as well as highlighting missing or deleted patient.



[issues-shield]: https://img.shields.io/badge/Issues-0-blue?style=for-the-badge
[issues-url]: https://renalregistry.atlassian.net/jira/software/projects/NHSBT/boards/19


