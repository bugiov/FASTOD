## What is Fastod?
Fastod is the subject of my Computer Science thesis and it's an algorithm which efficiently discovers a complete and minimal set of set-based ODs extracted from a dataset. This is done with [Java](https://www.java.com/it/download/) and [Lucene OpenBitset](https://lucene.apache.org/core/3_0_3/api/core/org/apache/lucene/util/OpenBitSet.html).

## Development
Development-related progress can be seen in the `develop` branch. Keep reading if you want to give it a try.

### Prerequisites
- Java v8
- Netbeans v8.2
- dataset in the csv format, with attributes separated by a semicolon (;) and with a header (find some datasets in this repository).

### Run
```bash
$ git clone https://github.com/buonincontrigi/FASTOD.git
```
- Open project folder into NetBeans and load all lib dependencies from "lib" folder.
- Enter the command line parameter in the netbeans settings to select the path of the dataset (complete with .csv extension) to be analyzed.

Datasets must be a .csv file structured as follows:
```
Name;Surname;Salary;Tax
Antonio;Auriemma;2000;200
Giacomo;Caliendo;4000;400
...
```
where Name, Surname, Salary and Tax are attributes and from line 2 onwards they are values for the attributes.

## License
Fastod is [Attribution-NonCommercial-NoDerivatives 4.0 International (CC BY-NC-ND 4.0)](./LICENSE).
