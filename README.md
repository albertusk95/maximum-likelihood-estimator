# Maximum Likelihood Estimation

A collection of maximum likelihood estimators for the fitted distribution parameters given a set of observations.

## Assumptions

* The observations were collected in independent and identically distributed (i.i.d) manner

The above assumption simplifies the generation of likelihood function

Given a set of observations (i.i.d), the likelihood of the distribution parameters is calculated as the following.

```
L(params | x1, x2, x3, ..., xn) = P(x1, x2, x3, ..., xn | params)

Since the observations are independent and identically distributed, we get the following.

L(params | x1, x2, x3, ..., xn) = P(x1 | params) . P(x2 | params) . P(x3 | params) ... P(xn | params)
```

## Quickstart

* Download the latest code version from the releases tab
* Create the configuration file. See the <a href="https://github.com/albertusk95/maximum-likelihood-estimator/blob/master/src/main/resources/exampleConfig.json">example</a>
* Run with `java -cp [path_to_application_jar] stats.MLE [path_to_config_file]`

## Language, Frameworks & Libraries

* Scala 2.11
* Spark 2.4.4
* Circe 0.12.0-M3 (JSON library for Scala)

## Configuration

Here's an example of the config file.

```
{
  "max_likelihood_estimates": [
    {
      "fitted_distribution": "normal",
      "column": "",
      "source": {
        "format": "csv",
        "path": ""
      }
    },
    {
      "fitted_distribution": "exp",
      "column": "",
      "source": {
        "format": "csv",
        "path": ""
      }
    }
  ]
}
```

### max_likelihood_estimates

A set of distribution MLEs

### fitted_distribution

The distribution whose parameters' MLE will be calculated by fitting it to the observations.

Currently supports:
* normal distribution: `normal`
* exponential distribution: `exp`

### column

The column name that provides a set of observations

### source

The information of the source data
* `format`: supported file formats are `csv` and `parquet`
* `path`: path to the source data
