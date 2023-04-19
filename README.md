# twapi-python-client
Python Client for interaction with the TaroWorks API


https://stackoverflow.com/questions/56658553/module-not-found-error-in-vs-code-despite-the-fact-that-i-installed-it

```
python3 -m venv env
source env/bin/activate
```

```
python3 -m pip install {new_module}
```

Note - as of 2023.04.18, several forms are not working from TSO6. These are explicitly included with the line:

```
if ((thisFormName != "DRC USAID Full Census 4.2") and (thisFormName != "B2p Project Assessment - New Site_DRAFT REVISIONS V3") and (thisFormName != "SISTEMA SALE04 - Data Collection & Quote") and (thisFormName != "Boma Monitoring Form")):
```
       
B2p Project Assessment - New Site_DRAFT REVISIONS V3.xlsx