## Installation

###### PyPI:

```pip install cimpyorm```

---
## Usage
```python
import cimpyorm
```

---
##### Loading datasets from cimpyorm-.db file
```python
session, m = cimpyorm.load(r"Path/To/DatabaseFile") # Load an existing .db file
```

---
##### Parsing datasets
```python
session, m = cimpyorm.parse(r"Path/To/Folder/Containing/Export") # Parse a .xml export (also creates a cimpyorm-.db file of the export)
```
To configure additional schemata (currently only the schema for the CGMES profiles are distributed
with the application), create additional subfolders in the ```/res/schemata/``` directory 
containg the schema RDFS.

---
##### Running the tests
You can run the included test-suite by running ```cimpyorm.test_all()```.

---
##### Querying datasets
```python
all_terminals = session.query(m.Terminal).all()
names_of_ConductingEquipment = [t.ConductingEquipment.name for t in all_terminals]
```

---
## Bug reports/feature requests
Please use the Issue Tracker.