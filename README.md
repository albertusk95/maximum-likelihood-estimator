# Maximum Likelihood Estimation (EMELY)

Hi, I'm <b>EMELY</b> (that's how you'd spell MLE),

A collection of maximum likelihood estimators for the fitted distribution parameters given a set of observations.

## Assumption

The observations were collected in independent and identically distributed (i.i.d) manner.

This assumption simplifies the generation of likelihood function.

Given a set of observations (i.i.d), the likelihood of the distribution parameters is calculated as the following.

`L(params | x1, x2, x3, ..., xn) = P(x1, x2, x3, ..., xn | params)`

Since the observations are independent and identically distributed, we get the following.

`L(params | x1, x2, x3, ..., xn) = P(x1 | params) . P(x2 | params) . P(x3 | params) ... P(xn | params)`

## Quickstart

* Download the latest version of <b>EMELY</b> from the releases tab
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
  "max_likelihood_estimates": {
    "normal": [
      {
        "column": "",
        "source": {
          "format": "csv",
          "path": ""
        }
      }
    ],
    "exp": [
      {
        "column": "",
        "source": {
          "format": "csv",
          "path": ""
        }
      }
    ],
    "binomial": [
      {
        "column": "",
        "success_event": "",
        "source": {
          "format": "csv",
          "path": ""
        }
      }
    ],
    "poisson": [
      {
        "column": "",
        "source": {
          "format": "csv",
          "path": ""
        }
      }
    ]
  }
}
```

### max_likelihood_estimates

A set of distribution MLEs

### fitted_distribution

The distribution whose parameters' MLE will be calculated by fitting it to the observations.

Currently supports:
* normal distribution: `normal`
* exponential distribution: `exp`
* binomial distribution: `binomial`
* poisson distribution: `poisson`

### column

The column name that provides a set of observations.

### success_event

For binomial distribution.

It denotes the observation whose probability will be calculated.

### source

The information of the source data:
* `format`: supported file formats are `csv` and `parquet`
* `path`: path to the source data

## Maths Notes

The MLEs are calculated by taking the derivative of the log-likelihood which respects to the parameter and setting it to zero.

```
L(params | x1, x2, x3, ..., xn) = P(x1 | params) . P(x2 | params) . P(x3 | params) ... P(xn | params)
```

Generating the log-likelihood function.

```
l = log(L(params | x1, x2, x3, ..., xn)) = log(P(x1 | params) . P(x2 | params) . P(x3 | params) ... P(xn | params))

l = log(L(params | x1, x2, x3, ..., xn)) = log(P(x1 | params)) + log(P(x2 | params)) + log(P(x3 | params)) + ... + log(P(xn | params))
```

Taking the derivative which respects to the parameter.

```
  dl   =  d(log(P(x1 | params))) + d(log(P(x2 | params))) + d(log(P(x3 | params))) + ... + d(log(P(xn | params)))
------    ----------------------   ----------------------   ----------------------         ----------------------
dparam           dparam                    dparam                   dparam                          dparam
```

Set the derivative to zero & create an equation for the param.

```
MLE of the param =    dl     =  0
                   --------
                    dparam
```

The derivation uses log-likelihood since besides it's easier to calculate, the resulting MLE is also the same with the one resulted from the original likelihood function.

A quick mathematical proof.

```
L = p(x,y) . q(x,y) . r(x,y)

L' =  dL  = d(p(x,y) . q(x,y) . r(x,y))
     ----   ---------------------------
      dx                dx

To find the MLE for x, the task becomes solving for L' = 0.

===

Now take the log of L.

log(L) = log(p(x,y) . q(x,y) . r(x,y))

Since dlog(f(x)) / dx = f'(x) / [f(x) ln(10)], then

dlog(L) = L' / (L ln(10))
-------
  dx
  
To find the MLE for x, the task becomes solving for L' / (L ln(10)) = 0.

It simply states that L' = 0 since the denominator can't be zero.
```

## Author

Albertus Kelvin, 2020
