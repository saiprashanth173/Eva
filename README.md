## EVA (Exploratory Video Analytics)

### Table of Contents
* Dataset
* Filters
* EVA core


### Dataset
__[dataset info](data/README.md)__ explains detailed information about the  datasets


### Filters
The below preprocessing method is running:
* PCA

The filters below are running:
* KDE
* DNN
* Random Forest
* SVM

To see the pipeline in action, execute the following commands:

```bash
   cd $YOUR_EVA_DIRECTORY
   python pipeline.py
```

### Getting started
0. If you don't have Anaconda, follow instructions here to install - (https://docs.continuum.io/anaconda/install/linux/)[here]
1. We'll setup our codebase in an Anaconda virtual env
```bash
conda create -n eva python=3 anaconda
source activate eva
```
2. Clone repo to target folder 
```bash
   git clone https://github.com/georgia-tech-db/Eva.git
```
3. Install all dependancies listed in requirements.txt
```bash
while read requirement; do conda install --yes $requirement; done < requirements.txt
```
4. Download and setup dataset
 - Follow instructions mentioned in dataset/ua_detrac
 - For fast prototyping on smaller dataset, create the folders **small-data**, **small-annotation**. 
 - Copy over one folder from **Insight-MVT_Annotation_Train** and **DETRAC-Train-Annotations-XML** to **small-data** and  **small-annotation**.
 - Specify paths to **small-data** and  **small-annotation** within filters/load.py
 - To verify if everything's setup, run
```bash
python filters/load.py
```

