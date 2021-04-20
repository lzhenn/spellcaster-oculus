## spellcaster-oculus

![](https://raw.githubusercontent.com/Novarizark/spellcaster-oculus/main/db/oculus.jpg)

**Spellcaster! Oculus** is a monthly scale anomaly prediction experiment focusing on temperature and precipitation. 
It used LASSO (L1 Regularization) to extract features from large amounts of circulation indicators and implement Random Forest to predict anomalies in 2000+ weather stations in China.

It is only an experimental project and provides no warranty.

**Spellcaster! Oculus (2021)** is the updated and refined version of **[Spellcaster-local (2019-20)](https://github.com/Novarizark/spellcaster-local)**. Also, _[The Oculus](http://classic.battle.net/diablo2exp/items/normal/usorceress.shtml)_ comes from the unique item specifically for Sorceress in DiabloII.

### File Structure

#### conf

* `config.ini`: config file controlling the whole run.

#### db
static data archive, including station meta, historical obvs.

#### feature_warehouse
potential features after ETL

#### lib
source code lib for model components

#### preload
external shell scripts that control the update of dynamic data (potential features)

#### raw_feature
dynamic data (potential features) before ETL 

