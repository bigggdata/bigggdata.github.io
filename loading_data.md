# Data Processing Workflow with Dask
## Loading Data Into DataFrames

![image.png](attachment:image.png)

### Before beginning, set your working directory to where the data resides


```python
import os
os.chdir('/Users/fitzroi/Library/Mobile Documents/com~apple~CloudDocs/Documents/FIT/teaching/fall_2022/big_data/plans/tutorials/dask/nyc_parking_violations')
```

### Load the datasets


```python
import dask.dataframe as dd
from dask.diagnostics import ProgressBar

fy14 = dd.read_csv('2014-2017/Parking_Violations_Issued_-_Fiscal_Year_2014__August_2013___June_2014_.csv')
fy15 = dd.read_csv('2014-2017/Parking_Violations_Issued_-_Fiscal_Year_2015.csv')
fy16 = dd.read_csv('2014-2017/Parking_Violations_Issued_-_Fiscal_Year_2016.csv')
fy17 = dd.read_csv('2014-2017/Parking_Violations_Issued_-_Fiscal_Year_2017.csv')
fy17
```




<div><strong>Dask DataFrame Structure:</strong></div>
<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Summons Number</th>
      <th>Plate ID</th>
      <th>Registration State</th>
      <th>Plate Type</th>
      <th>Issue Date</th>
      <th>Violation Code</th>
      <th>Vehicle Body Type</th>
      <th>Vehicle Make</th>
      <th>Issuing Agency</th>
      <th>Street Code1</th>
      <th>Street Code2</th>
      <th>Street Code3</th>
      <th>Vehicle Expiration Date</th>
      <th>Violation Location</th>
      <th>Violation Precinct</th>
      <th>Issuer Precinct</th>
      <th>Issuer Code</th>
      <th>Issuer Command</th>
      <th>Issuer Squad</th>
      <th>Violation Time</th>
      <th>Time First Observed</th>
      <th>Violation County</th>
      <th>Violation In Front Of Or Opposite</th>
      <th>House Number</th>
      <th>Street Name</th>
      <th>Intersecting Street</th>
      <th>Date First Observed</th>
      <th>Law Section</th>
      <th>Sub Division</th>
      <th>Violation Legal Code</th>
      <th>Days Parking In Effect</th>
      <th>From Hours In Effect</th>
      <th>To Hours In Effect</th>
      <th>Vehicle Color</th>
      <th>Unregistered Vehicle?</th>
      <th>Vehicle Year</th>
      <th>Meter Number</th>
      <th>Feet From Curb</th>
      <th>Violation Post Code</th>
      <th>Violation Description</th>
      <th>No Standing or Stopping Violation</th>
      <th>Hydrant Violation</th>
      <th>Double Parking Violation</th>
    </tr>
    <tr>
      <th>npartitions=33</th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th></th>
      <td>int64</td>
      <td>object</td>
      <td>object</td>
      <td>object</td>
      <td>object</td>
      <td>int64</td>
      <td>object</td>
      <td>object</td>
      <td>object</td>
      <td>int64</td>
      <td>int64</td>
      <td>int64</td>
      <td>int64</td>
      <td>float64</td>
      <td>int64</td>
      <td>int64</td>
      <td>int64</td>
      <td>object</td>
      <td>object</td>
      <td>object</td>
      <td>float64</td>
      <td>object</td>
      <td>object</td>
      <td>float64</td>
      <td>object</td>
      <td>object</td>
      <td>int64</td>
      <td>int64</td>
      <td>object</td>
      <td>object</td>
      <td>object</td>
      <td>object</td>
      <td>object</td>
      <td>object</td>
      <td>float64</td>
      <td>int64</td>
      <td>object</td>
      <td>int64</td>
      <td>object</td>
      <td>object</td>
      <td>float64</td>
      <td>float64</td>
      <td>float64</td>
    </tr>
    <tr>
      <th></th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th></th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th></th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
  </tbody>
</table>
</div>
<div>Dask Name: read-csv, 33 tasks</div>



### List the Columns


```python
fy17.columns
```




    Index(['Summons Number', 'Plate ID', 'Registration State', 'Plate Type',
           'Issue Date', 'Violation Code', 'Vehicle Body Type', 'Vehicle Make',
           'Issuing Agency', 'Street Code1', 'Street Code2', 'Street Code3',
           'Vehicle Expiration Date', 'Violation Location', 'Violation Precinct',
           'Issuer Precinct', 'Issuer Code', 'Issuer Command', 'Issuer Squad',
           'Violation Time', 'Time First Observed', 'Violation County',
           'Violation In Front Of Or Opposite', 'House Number', 'Street Name',
           'Intersecting Street', 'Date First Observed', 'Law Section',
           'Sub Division', 'Violation Legal Code', 'Days Parking In Effect    ',
           'From Hours In Effect', 'To Hours In Effect', 'Vehicle Color',
           'Unregistered Vehicle?', 'Vehicle Year', 'Meter Number',
           'Feet From Curb', 'Violation Post Code', 'Violation Description',
           'No Standing or Stopping Violation', 'Hydrant Violation',
           'Double Parking Violation'],
          dtype='object')




```python
# Finding common columns in datasets
from functools import reduce

columns = [set(fy14.columns),
    set(fy15.columns),
    set(fy16.columns),
    set(fy17.columns)]
common_columns = list(reduce(lambda a, i: a.intersection(i), columns))
common_columns
```




    ['Violation In Front Of Or Opposite',
     'Issuer Precinct',
     'Registration State',
     'Street Code2',
     'Violation Location',
     'Time First Observed',
     'Intersecting Street',
     'Unregistered Vehicle?',
     'Vehicle Body Type',
     'Sub Division',
     'Violation Post Code',
     'Issuer Code',
     'Plate ID',
     'Street Code3',
     'Violation Precinct',
     'Vehicle Color',
     'Violation Legal Code',
     'Law Section',
     'Violation Time',
     'Vehicle Year',
     'To Hours In Effect',
     'Hydrant Violation',
     'Summons Number',
     'Vehicle Make',
     'No Standing or Stopping Violation',
     'Double Parking Violation',
     'Issuer Command',
     'Issue Date',
     'Street Code1',
     'Violation Description',
     'Date First Observed',
     'House Number',
     'Meter Number',
     'Days Parking In Effect    ',
     'Violation Code',
     'Street Name',
     'Feet From Curb',
     'Vehicle Expiration Date',
     'Plate Type',
     'From Hours In Effect',
     'Violation County',
     'Issuer Squad',
     'Issuing Agency']



### Demonstrating Errors with Dask Type Inferencing


```python
fy17[common_columns].head()
```


    ------------------------------------------------------------

    ValueError                 Traceback (most recent call last)

    <ipython-input-13-8a4a98a942c8> in <module>
    ----> 1 fy17[common_columns].head()
    

    ~/opt/anaconda3/lib/python3.8/site-packages/dask/dataframe/core.py in head(self, n, npartitions, compute)
       1068             Whether to compute the result, default is True.
       1069         """
    -> 1070         return self._head(n=n, npartitions=npartitions, compute=compute, safe=True)
       1071 
       1072     def _head(self, n, npartitions, compute, safe):


    ~/opt/anaconda3/lib/python3.8/site-packages/dask/dataframe/core.py in _head(self, n, npartitions, compute, safe)
       1101 
       1102         if compute:
    -> 1103             result = result.compute()
       1104         return result
       1105 


    ~/opt/anaconda3/lib/python3.8/site-packages/dask/base.py in compute(self, **kwargs)
        284         dask.base.compute
        285         """
    --> 286         (result,) = compute(self, traverse=False, **kwargs)
        287         return result
        288 


    ~/opt/anaconda3/lib/python3.8/site-packages/dask/base.py in compute(*args, **kwargs)
        566         postcomputes.append(x.__dask_postcompute__())
        567 
    --> 568     results = schedule(dsk, keys, **kwargs)
        569     return repack([f(r, *a) for r, (f, a) in zip(results, postcomputes)])
        570 


    ~/opt/anaconda3/lib/python3.8/site-packages/dask/threaded.py in get(dsk, result, cache, num_workers, pool, **kwargs)
         77             pool = MultiprocessingPoolExecutor(pool)
         78 
    ---> 79     results = get_async(
         80         pool.submit,
         81         pool._max_workers,


    ~/opt/anaconda3/lib/python3.8/site-packages/dask/local.py in get_async(submit, num_workers, dsk, result, cache, get_id, rerun_exceptions_locally, pack_exception, raise_exception, callbacks, dumps, loads, chunksize, **kwargs)
        512                             _execute_task(task, data)  # Re-execute locally
        513                         else:
    --> 514                             raise_exception(exc, tb)
        515                     res, worker_id = loads(res_info)
        516                     state["cache"][key] = res


    ~/opt/anaconda3/lib/python3.8/site-packages/dask/local.py in reraise(exc, tb)
        323     if exc.__traceback__ is not tb:
        324         raise exc.with_traceback(tb)
    --> 325     raise exc
        326 
        327 


    ~/opt/anaconda3/lib/python3.8/site-packages/dask/local.py in execute_task(key, task_info, dumps, loads, get_id, pack_exception)
        221     try:
        222         task, data = loads(task_info)
    --> 223         result = _execute_task(task, data)
        224         id = get_id()
        225         result = dumps((result, id))


    ~/opt/anaconda3/lib/python3.8/site-packages/dask/core.py in _execute_task(arg, cache, dsk)
        119         # temporaries by their reference count and can execute certain
        120         # operations in-place.
    --> 121         return func(*(_execute_task(a, cache) for a in args))
        122     elif not ishashable(arg):
        123         return arg


    ~/opt/anaconda3/lib/python3.8/site-packages/dask/optimization.py in __call__(self, *args)
        967         if not len(args) == len(self.inkeys):
        968             raise ValueError("Expected %d args, got %d" % (len(self.inkeys), len(args)))
    --> 969         return core.get(self.dsk, self.outkey, dict(zip(self.inkeys, args)))
        970 
        971     def __reduce__(self):


    ~/opt/anaconda3/lib/python3.8/site-packages/dask/core.py in get(dsk, out, cache)
        149     for key in toposort(dsk):
        150         task = dsk[key]
    --> 151         result = _execute_task(task, cache)
        152         cache[key] = result
        153     result = _execute_task(out, cache)


    ~/opt/anaconda3/lib/python3.8/site-packages/dask/core.py in _execute_task(arg, cache, dsk)
        119         # temporaries by their reference count and can execute certain
        120         # operations in-place.
    --> 121         return func(*(_execute_task(a, cache) for a in args))
        122     elif not ishashable(arg):
        123         return arg


    ~/opt/anaconda3/lib/python3.8/site-packages/dask/core.py in <genexpr>(.0)
        119         # temporaries by their reference count and can execute certain
        120         # operations in-place.
    --> 121         return func(*(_execute_task(a, cache) for a in args))
        122     elif not ishashable(arg):
        123         return arg


    ~/opt/anaconda3/lib/python3.8/site-packages/dask/core.py in _execute_task(arg, cache, dsk)
        119         # temporaries by their reference count and can execute certain
        120         # operations in-place.
    --> 121         return func(*(_execute_task(a, cache) for a in args))
        122     elif not ishashable(arg):
        123         return arg


    ~/opt/anaconda3/lib/python3.8/site-packages/dask/dataframe/io/csv.py in __call__(self, part)
        123 
        124         # Call `pandas_read_text`
    --> 125         df = pandas_read_text(
        126             self.reader,
        127             block,


    ~/opt/anaconda3/lib/python3.8/site-packages/dask/dataframe/io/csv.py in pandas_read_text(reader, b, header, kwargs, dtypes, columns, write_header, enforce, path)
        178     df = reader(bio, **kwargs)
        179     if dtypes:
    --> 180         coerce_dtypes(df, dtypes)
        181 
        182     if enforce and columns and (list(df.columns) != list(columns)):


    ~/opt/anaconda3/lib/python3.8/site-packages/dask/dataframe/io/csv.py in coerce_dtypes(df, dtypes)
        280             rule.join(filter(None, [dtype_msg, date_msg]))
        281         )
    --> 282         raise ValueError(msg)
        283 
        284 


    ValueError: Mismatched dtypes found in `pd.read_csv`/`pd.read_table`.
    
    +---------------------+--------+----------+
    | Column              | Found  | Expected |
    +---------------------+--------+----------+
    | House Number        | object | float64  |
    | Time First Observed | object | float64  |
    +---------------------+--------+----------+
    
    The following columns also raised exceptions on conversion:
    
    - House Number
      ValueError("could not convert string to float: '150-34'")
    - Time First Observed
      ValueError("could not convert string to float: '1138A'")
    
    Usually this is due to dask's dtype inference failing, and
    *may* be fixed by specifying dtypes manually by adding:
    
    dtype={'House Number': 'object',
           'Time First Observed': 'object'}
    
    to the call to `read_csv`/`read_table`.



```python
# Note: This is supposed to produce an error - scroll down and inspect the ValueError
fy14[common_columns].head()
```

    /Users/fitzroi/opt/anaconda3/lib/python3.8/site-packages/dask/dataframe/io/csv.py:125: DtypeWarning: Columns (18,29,38,39) have mixed types.Specify dtype option on import or set low_memory=False.
      df = pandas_read_text(



    ------------------------------------------------------------

    ValueError                 Traceback (most recent call last)

    <ipython-input-8-5518ad2a9462> in <module>
          1 # Listing 4.5
          2 # Note: This is supposed to produce an error - scroll down and inspect the ValueError
    ----> 3 fy14[common_columns].head()
    

    ~/opt/anaconda3/lib/python3.8/site-packages/dask/dataframe/core.py in head(self, n, npartitions, compute)
       1068             Whether to compute the result, default is True.
       1069         """
    -> 1070         return self._head(n=n, npartitions=npartitions, compute=compute, safe=True)
       1071 
       1072     def _head(self, n, npartitions, compute, safe):


    ~/opt/anaconda3/lib/python3.8/site-packages/dask/dataframe/core.py in _head(self, n, npartitions, compute, safe)
       1101 
       1102         if compute:
    -> 1103             result = result.compute()
       1104         return result
       1105 


    ~/opt/anaconda3/lib/python3.8/site-packages/dask/base.py in compute(self, **kwargs)
        284         dask.base.compute
        285         """
    --> 286         (result,) = compute(self, traverse=False, **kwargs)
        287         return result
        288 


    ~/opt/anaconda3/lib/python3.8/site-packages/dask/base.py in compute(*args, **kwargs)
        566         postcomputes.append(x.__dask_postcompute__())
        567 
    --> 568     results = schedule(dsk, keys, **kwargs)
        569     return repack([f(r, *a) for r, (f, a) in zip(results, postcomputes)])
        570 


    ~/opt/anaconda3/lib/python3.8/site-packages/dask/threaded.py in get(dsk, result, cache, num_workers, pool, **kwargs)
         77             pool = MultiprocessingPoolExecutor(pool)
         78 
    ---> 79     results = get_async(
         80         pool.submit,
         81         pool._max_workers,


    ~/opt/anaconda3/lib/python3.8/site-packages/dask/local.py in get_async(submit, num_workers, dsk, result, cache, get_id, rerun_exceptions_locally, pack_exception, raise_exception, callbacks, dumps, loads, chunksize, **kwargs)
        512                             _execute_task(task, data)  # Re-execute locally
        513                         else:
    --> 514                             raise_exception(exc, tb)
        515                     res, worker_id = loads(res_info)
        516                     state["cache"][key] = res


    ~/opt/anaconda3/lib/python3.8/site-packages/dask/local.py in reraise(exc, tb)
        323     if exc.__traceback__ is not tb:
        324         raise exc.with_traceback(tb)
    --> 325     raise exc
        326 
        327 


    ~/opt/anaconda3/lib/python3.8/site-packages/dask/local.py in execute_task(key, task_info, dumps, loads, get_id, pack_exception)
        221     try:
        222         task, data = loads(task_info)
    --> 223         result = _execute_task(task, data)
        224         id = get_id()
        225         result = dumps((result, id))


    ~/opt/anaconda3/lib/python3.8/site-packages/dask/core.py in _execute_task(arg, cache, dsk)
        119         # temporaries by their reference count and can execute certain
        120         # operations in-place.
    --> 121         return func(*(_execute_task(a, cache) for a in args))
        122     elif not ishashable(arg):
        123         return arg


    ~/opt/anaconda3/lib/python3.8/site-packages/dask/optimization.py in __call__(self, *args)
        967         if not len(args) == len(self.inkeys):
        968             raise ValueError("Expected %d args, got %d" % (len(self.inkeys), len(args)))
    --> 969         return core.get(self.dsk, self.outkey, dict(zip(self.inkeys, args)))
        970 
        971     def __reduce__(self):


    ~/opt/anaconda3/lib/python3.8/site-packages/dask/core.py in get(dsk, out, cache)
        149     for key in toposort(dsk):
        150         task = dsk[key]
    --> 151         result = _execute_task(task, cache)
        152         cache[key] = result
        153     result = _execute_task(out, cache)


    ~/opt/anaconda3/lib/python3.8/site-packages/dask/core.py in _execute_task(arg, cache, dsk)
        119         # temporaries by their reference count and can execute certain
        120         # operations in-place.
    --> 121         return func(*(_execute_task(a, cache) for a in args))
        122     elif not ishashable(arg):
        123         return arg


    ~/opt/anaconda3/lib/python3.8/site-packages/dask/core.py in <genexpr>(.0)
        119         # temporaries by their reference count and can execute certain
        120         # operations in-place.
    --> 121         return func(*(_execute_task(a, cache) for a in args))
        122     elif not ishashable(arg):
        123         return arg


    ~/opt/anaconda3/lib/python3.8/site-packages/dask/core.py in _execute_task(arg, cache, dsk)
        119         # temporaries by their reference count and can execute certain
        120         # operations in-place.
    --> 121         return func(*(_execute_task(a, cache) for a in args))
        122     elif not ishashable(arg):
        123         return arg


    ~/opt/anaconda3/lib/python3.8/site-packages/dask/dataframe/io/csv.py in __call__(self, part)
        123 
        124         # Call `pandas_read_text`
    --> 125         df = pandas_read_text(
        126             self.reader,
        127             block,


    ~/opt/anaconda3/lib/python3.8/site-packages/dask/dataframe/io/csv.py in pandas_read_text(reader, b, header, kwargs, dtypes, columns, write_header, enforce, path)
        178     df = reader(bio, **kwargs)
        179     if dtypes:
    --> 180         coerce_dtypes(df, dtypes)
        181 
        182     if enforce and columns and (list(df.columns) != list(columns)):


    ~/opt/anaconda3/lib/python3.8/site-packages/dask/dataframe/io/csv.py in coerce_dtypes(df, dtypes)
        280             rule.join(filter(None, [dtype_msg, date_msg]))
        281         )
    --> 282         raise ValueError(msg)
        283 
        284 


    ValueError: Mismatched dtypes found in `pd.read_csv`/`pd.read_table`.
    
    +-----------------------+---------+----------+
    | Column                | Found   | Expected |
    +-----------------------+---------+----------+
    | House Number          | object  | float64  |
    | Issuer Command        | object  | int64    |
    | Issuer Squad          | object  | int64    |
    | Time First Observed   | object  | float64  |
    | Unregistered Vehicle? | float64 | int64    |
    | Violation Description | object  | float64  |
    | Violation Legal Code  | object  | float64  |
    | Violation Location    | float64 | int64    |
    | Violation Post Code   | object  | float64  |
    +-----------------------+---------+----------+
    
    The following columns also raised exceptions on conversion:
    
    - House Number
      ValueError("could not convert string to float: '67-21'")
    - Issuer Command
      ValueError("invalid literal for int() with base 10: 'T730'")
    - Issuer Squad
      ValueError('cannot convert float NaN to integer')
    - Time First Observed
      ValueError("could not convert string to float: '1134P'")
    - Violation Description
      ValueError("could not convert string to float: 'BUS LANE VIOLATION'")
    - Violation Legal Code
      ValueError("could not convert string to float: 'T'")
    - Violation Post Code
      ValueError("could not convert string to float: 'H -'")
    
    Usually this is due to dask's dtype inference failing, and
    *may* be fixed by specifying dtypes manually by adding:
    
    dtype={'House Number': 'object',
           'Issuer Command': 'object',
           'Issuer Squad': 'object',
           'Time First Observed': 'object',
           'Unregistered Vehicle?': 'float64',
           'Violation Description': 'object',
           'Violation Legal Code': 'object',
           'Violation Location': 'float64',
           'Violation Post Code': 'object'}
    
    to the call to `read_csv`/`read_table`.


### Building a Generic Schema


```python
import numpy as np
import pandas as pd

dtype_tuples = [(x, np.str_) for x in common_columns]
dtypes = dict(dtype_tuples)
dtypes
```




    {'Violation In Front Of Or Opposite': numpy.str_,
     'Issuer Precinct': numpy.str_,
     'Registration State': numpy.str_,
     'Street Code2': numpy.str_,
     'Violation Location': numpy.str_,
     'Time First Observed': numpy.str_,
     'Intersecting Street': numpy.str_,
     'Unregistered Vehicle?': numpy.str_,
     'Vehicle Body Type': numpy.str_,
     'Sub Division': numpy.str_,
     'Violation Post Code': numpy.str_,
     'Issuer Code': numpy.str_,
     'Plate ID': numpy.str_,
     'Street Code3': numpy.str_,
     'Violation Precinct': numpy.str_,
     'Vehicle Color': numpy.str_,
     'Violation Legal Code': numpy.str_,
     'Law Section': numpy.str_,
     'Violation Time': numpy.str_,
     'Vehicle Year': numpy.str_,
     'To Hours In Effect': numpy.str_,
     'Hydrant Violation': numpy.str_,
     'Summons Number': numpy.str_,
     'Vehicle Make': numpy.str_,
     'No Standing or Stopping Violation': numpy.str_,
     'Double Parking Violation': numpy.str_,
     'Issuer Command': numpy.str_,
     'Issue Date': numpy.str_,
     'Street Code1': numpy.str_,
     'Violation Description': numpy.str_,
     'Date First Observed': numpy.str_,
     'House Number': numpy.str_,
     'Meter Number': numpy.str_,
     'Days Parking In Effect    ': numpy.str_,
     'Violation Code': numpy.str_,
     'Street Name': numpy.str_,
     'Feet From Curb': numpy.str_,
     'Vehicle Expiration Date': numpy.str_,
     'Plate Type': numpy.str_,
     'From Hours In Effect': numpy.str_,
     'Violation County': numpy.str_,
     'Issuer Squad': numpy.str_,
     'Issuing Agency': numpy.str_}



### Reading the Data with the Generic Schema


```python
fy14 = dd.read_csv('2014-2017/Parking_Violations_Issued_-_Fiscal_Year_2014__August_2013___June_2014_.csv', dtype=dtypes)

with ProgressBar():
    display(fy14[common_columns].head())
```

    [########################################] | 100% Completed |  1.5s



<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Violation In Front Of Or Opposite</th>
      <th>Issuer Precinct</th>
      <th>Registration State</th>
      <th>Street Code2</th>
      <th>Violation Location</th>
      <th>Time First Observed</th>
      <th>Intersecting Street</th>
      <th>Unregistered Vehicle?</th>
      <th>Vehicle Body Type</th>
      <th>Sub Division</th>
      <th>...</th>
      <th>Days Parking In Effect</th>
      <th>Violation Code</th>
      <th>Street Name</th>
      <th>Feet From Curb</th>
      <th>Vehicle Expiration Date</th>
      <th>Plate Type</th>
      <th>From Hours In Effect</th>
      <th>Violation County</th>
      <th>Issuer Squad</th>
      <th>Issuing Agency</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>F</td>
      <td>33</td>
      <td>NY</td>
      <td>13610</td>
      <td>0033</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>0</td>
      <td>SUBN</td>
      <td>F1</td>
      <td>...</td>
      <td>BBBBBBB</td>
      <td>46</td>
      <td>W 175 ST</td>
      <td>0</td>
      <td>20140831</td>
      <td>PAS</td>
      <td>ALL</td>
      <td>NaN</td>
      <td>0000</td>
      <td>P</td>
    </tr>
    <tr>
      <th>1</th>
      <td>O</td>
      <td>33</td>
      <td>NY</td>
      <td>40404</td>
      <td>0033</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>0</td>
      <td>VAN</td>
      <td>C</td>
      <td>...</td>
      <td>BBBBBBB</td>
      <td>46</td>
      <td>W 177 ST</td>
      <td>0</td>
      <td>20140430</td>
      <td>COM</td>
      <td>ALL</td>
      <td>NY</td>
      <td>0000</td>
      <td>P</td>
    </tr>
    <tr>
      <th>2</th>
      <td>O</td>
      <td>33</td>
      <td>NY</td>
      <td>31190</td>
      <td>0033</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>0</td>
      <td>P-U</td>
      <td>F7</td>
      <td>...</td>
      <td>BBBBBBB</td>
      <td>46</td>
      <td>W 163 ST</td>
      <td>0</td>
      <td>20140228</td>
      <td>COM</td>
      <td>ALL</td>
      <td>NY</td>
      <td>0000</td>
      <td>P</td>
    </tr>
    <tr>
      <th>3</th>
      <td>O</td>
      <td>33</td>
      <td>NY</td>
      <td>11710</td>
      <td>0033</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>0</td>
      <td>VAN</td>
      <td>F1</td>
      <td>...</td>
      <td>BBBBBBB</td>
      <td>46</td>
      <td>W 176 ST</td>
      <td>0</td>
      <td>20141031</td>
      <td>COM</td>
      <td>ALL</td>
      <td>NY</td>
      <td>0000</td>
      <td>P</td>
    </tr>
    <tr>
      <th>4</th>
      <td>F</td>
      <td>33</td>
      <td>NY</td>
      <td>12010</td>
      <td>0033</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>0</td>
      <td>TRLR</td>
      <td>E1</td>
      <td>...</td>
      <td>BBBBBBB</td>
      <td>41</td>
      <td>W 174 ST</td>
      <td>0</td>
      <td>0</td>
      <td>COM</td>
      <td>ALL</td>
      <td>NY</td>
      <td>0000</td>
      <td>P</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 43 columns</p>
</div>



```python
with ProgressBar():
    print(fy14['Vehicle Year'].unique().head(10))
```

    [########################################] | 100% Completed | 27.3s
    0    2013
    1    2012
    2       0
    3    2010
    4    2011
    5    2001
    6    2005
    7    1998
    8    1995
    9    2003
    Name: Vehicle Year, dtype: object



```python
with ProgressBar():
    print(fy14['Vehicle Year'].isnull().values.any().compute())
```

    [########################################] | 100% Completed |  1min 13.6s
    True



```python
dtypes = {
 'Date First Observed': np.str_,
 'Days Parking In Effect    ': np.str_,
 'Double Parking Violation': np.str_,
 'Feet From Curb': np.float32,
 'From Hours In Effect': np.str_,
 'House Number': np.str_,
 'Hydrant Violation': np.str_,
 'Intersecting Street': np.str_,
 'Issue Date': np.str_,
 'Issuer Code': np.float32,
 'Issuer Command': np.str_,
 'Issuer Precinct': np.float32,
 'Issuer Squad': np.str_,
 'Issuing Agency': np.str_,
 'Law Section': np.float32,
 'Meter Number': np.str_,
 'No Standing or Stopping Violation': np.str_,
 'Plate ID': np.str_,
 'Plate Type': np.str_,
 'Registration State': np.str_,
 'Street Code1': np.uint32,
 'Street Code2': np.uint32,
 'Street Code3': np.uint32,
 'Street Name': np.str_,
 'Sub Division': np.str_,
 'Summons Number': np.uint32,
 'Time First Observed': np.str_,
 'To Hours In Effect': np.str_,
 'Unregistered Vehicle?': np.str_,
 'Vehicle Body Type': np.str_,
 'Vehicle Color': np.str_,
 'Vehicle Expiration Date': np.str_,
 'Vehicle Make': np.str_,
 'Vehicle Year': np.float32,
 'Violation Code': np.uint16,
 'Violation County': np.str_,
 'Violation Description': np.str_,
 'Violation In Front Of Or Opposite': np.str_,
 'Violation Legal Code': np.str_,
 'Violation Location': np.str_,
 'Violation Post Code': np.str_,
 'Violation Precinct': np.float32,
 'Violation Time': np.str_
}
```


```python
# Read data using the dtypes
data = dd.read_csv('2014-2017/*.csv', dtype=dtypes, usecols=common_columns)
```

### Ways to Read Data into Dask


```python
username = 'drfitz'
password = 'ICantLetYouThrowYourselfAway'
hostname = 'localhost'
database_name = 'DSAS'
odbc_driver = 'ODBC+Driver+13+for+SQL+Server'

#We may use pyodbc to manage several ODBC drivers
connection_string = 'mssql+pyodbc://{0}:{1}@{2}/{3}?driver={4}'.format(username, password, hostname, database_name, odbc_driver)

data = dd.read_sql_table('violations', connection_string, index_col='Summons Number')
```


```python
#Even partitioning on a non-numeric or date/time index
data = dd.read_sql_table('violations', connection_string, index_col='Vehicle Color', npartitions=200)
```


```python
#Custom partitioning on a non-numeric or date/time index
partition_boundaries = sorted(['Red', 'Blue', 'White', 'Black', 'Silver', 'Yellow'])

data = dd.read_sql_table('violations', connection_string, index_col='Vehicle Color', divisions=partition_boundaries)
```


```python
#Selecting a Subset of Columns
column_filter = ['Summons Number', 'Plate ID', 'Vehicle Color']
data = dd.read_sql_table('violations', connection_string, index_col='Summons Number', columns=column_filter)
```


```python
# Selecting a Schema
data = dd.read_sql_table('violations', connection_string, index_col='Summons Number', schema='chapterFour')
```

### Reading data from HDFS and S3


```python
# hdfs
data = dd.read_csv('hdfs://localhost/nyc-parking-tickets/*.csv', dtype=dtypes, usecols=common_columns)
```


```python
# s3
data = dd.read_csv('s3://my-bucket/nyc-parking-tickets/*.csv', dtype=dtypes, usecols=common_columns)
```

### Reading data from Parquet Files


```python
# Local parquet
data = dd.read_parquet('nyc-parking-tickets-prq')
```


```python
# HDFS and S3 parquet
data = dd.read_parquet('hdfs://localhost/nyc-parking-tickets-prq')

# OR

data = dd.read_parquet('s3://my-bucket/nyc-parking-tickets-prq')
```


```python
# Specifying columns in parquet
columms = ['Summons Number', 'Plate ID', 'Vehicle Color']
data = dd.read_parquet('nyc-parking-tickets-prq', columns=columns, index='Plate ID')
```
