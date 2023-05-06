<h1 align="center">
  <a>Gedaq</a>
</h1>

<h3 align="center">

  [![Nuget](https://img.shields.io/nuget/v/Gedaq?logo=Gedaq)](https://www.nuget.org/packages/Gedaq/)
  [![Downloads](https://img.shields.io/nuget/dt/Gedaq.svg)](https://www.nuget.org/packages/Gedaq/)
  [![Stars](https://img.shields.io/github/stars/SoftStoneDevelop/Gedaq?color=brightgreen)](https://github.com/SoftStoneDevelop/Gedaq/stargazers)
  [![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

</h3>

Generator of methods for obtaining data from databases.
Supported databases(see examples and documentation in the relevant DB package):<br>
- [Npgsql](https://github.com/SoftStoneDevelop/Gedaq.Npgsql)
- [DbConnection](https://github.com/SoftStoneDevelop/Gedaq.DbConnection)
- [SqlClient](https://github.com/SoftStoneDevelop/Gedaq.SqlClient)
- [MySqlConnector](https://github.com/SoftStoneDevelop/Gedaq.MySqlConnector)
- [Oracle.ManagedDataAccess.Core](https://github.com/SoftStoneDevelop/Gedaq/issues/6)

Usage:

For example, we have a Person class:
```C#

public class Person
{
    public int Id { get; set; }

    public string FirstName { get; set; }

    public string MiddleName { get; set; }

    public string LastName { get; set; }
    
    public Identification Identification { get; set; }
}

public class Identification
{
    public int Id { get; set; }
    public string TypeName { get; set; }
}

```
We just mark anywhere in the code with a special attribute (class, structure, method) that tells the analyzer to generate the data acquisition code.
Let's mark the Person class itself with an attribute:

```C#

[Gedaq.Npgsql.Attributes.Query(
            @"
SELECT 
    p.id,
    p.firstname,
~StartInner::Identification:id~
    i.id,
    i.typename,
~EndInner::Identification~
    p.middlename,
    p.lastname
FROM person p
LEFT JOIN identification i ON i.id = p.identification_id
WHERE p.id > $1
",
        "GetAllPerson",
        typeof(Person),
        MethodType.Sync | MethodType.Async
        )]
[Parametr("GetAllPerson", parametrType: typeof(int), position: 1)]
public class Person
//...

```

Now in the code we can call the ready method:
```C#

var persons = 
        connection
        .GetAllPerson(49999)
        .ToList();
        
var personsAsync = 
        await connection
        .GetAllPersonAsync(49999)
        .ToListAsync();

```

Comparison of getting 50000 Person in a loop(Size is number of loop iterations) from the database:


|                                         Method | Size |       Mean | Ratio |       Gen0 |       Gen1 |       Gen2 | Allocated | Alloc Ratio |
|----------------------------------------------- |----- |-----------:|------:|-----------:|-----------:|-----------:|----------:|------------:|
|                                          **Gedaq** |   **10** |   **404.1 ms** |  **0.62** | **17000.0000** | **16000.0000** |  **6000.0000** | **119.12 MB** |        **0.83** |
| &#39;Dapper.Query&lt;Person, Identification, Person&gt;&#39; |   10 |   703.3 ms |  1.00 | 21000.0000 | 20000.0000 |  7000.0000 | 143.93 MB |        1.00 |
|                                                |      |            |       |            |            |            |           |             |
|                                          **Gedaq** |   **20** |   **792.5 ms** |  **0.58** | **35000.0000** | **34000.0000** | **13000.0000** | **238.25 MB** |        **0.83** |
| &#39;Dapper.Query&lt;Person, Identification, Person&gt;&#39; |   20 | 1,372.9 ms |  1.00 | 43000.0000 | 42000.0000 | 16000.0000 | 287.85 MB |        1.00 |
|                                                |      |            |       |            |            |            |           |             |
|                                          **Gedaq** |   **30** | **1,221.7 ms** |  **0.58** | **54000.0000** | **53000.0000** | **20000.0000** | **357.36 MB** |        **0.83** |
| &#39;Dapper.Query&lt;Person, Identification, Person&gt;&#39; |   30 | 2,097.7 ms |  1.00 | 66000.0000 | 65000.0000 | 24000.0000 | 431.78 MB |        1.00 |
