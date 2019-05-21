=======================
Linter
=======================
CIMPyORM supports linting CIM datasets against their schema definition using the ``lint`` function:

.. autofunction:: cimpyorm.lint

The function yields a pandas pivot_table containing information about schema violations and model
inconsistencies, namely:

* Missing non-optional values (see :any:`Exploring` on how to determine which fields are optional)
* Missing references
* Invalid references (foreign-key reference not found in referenced table)

Linting the ENTSO-E example *FullGrid* dataset against the CGMES 2.4.15 specification yields the following violations:

.. raw:: html

    <embed>
        <table border="1" class="dataframe">
            <thead>
                <tr style="text-align: right;">
                <th></th>
                <th></th>
                <th></th>
                <th></th>
                <th>Unique violations</th>
                <th>Total violations</th>
                </tr>
                <tr>
                <th>Type</th>
                <th>Class</th>
                <th>Total</th>
                <th>Property</th>
                <th></th>
                <th></th>
                </tr>
            </thead>
            <tbody>
                <tr>
                <th rowspan="3" valign="top">Invalid</th>
                <th>SvVoltage</th>
                <th>25</th>
                <th>TopologicalNode</th>
                <td>5.0</td>
                <td>5</td>
                </tr>
                <tr>
                <th rowspan="2" valign="top">Terminal</th>
                <th rowspan="2" valign="top">144</th>
                <th>ConnectivityNode</th>
                <td>5.0</td>
                <td>10</td>
                </tr>
                <tr>
                <th>TopologicalNode</th>
                <td>6.0</td>
                <td>10</td>
                </tr>
                <tr>
                <th rowspan="18" valign="top">Missing</th>
                <th rowspan="7" valign="top">ConnectivityNode</th>
                <th rowspan="7" valign="top">40</th>
                <th>entsoe_boundaryPoint</th>
                <td>NaN</td>
                <td>40</td>
                </tr>
                <tr>
                <th>entsoe_fromEndIsoCode</th>
                <td>NaN</td>
                <td>40</td>
                </tr>
                <tr>
                <th>entsoe_fromEndName</th>
                <td>NaN</td>
                <td>40</td>
                </tr>
                <tr>
                <th>entsoe_fromEndNameTso</th>
                <td>NaN</td>
                <td>40</td>
                </tr>
                <tr>
                <th>entsoe_toEndIsoCode</th>
                <td>NaN</td>
                <td>40</td>
                </tr>
                <tr>
                <th>entsoe_toEndName</th>
                <td>NaN</td>
                <td>40</td>
                </tr>
                <tr>
                <th>entsoe_toEndNameTso</th>
                <td>NaN</td>
                <td>40</td>
                </tr>
                <tr>
                <th rowspan="2" valign="top">IdentifiedObject</th>
                <th rowspan="2" valign="top">731</th>
                <th>description</th>
                <td>NaN</td>
                <td>2</td>
                </tr>
                <tr>
                <th>entsoe_shortName</th>
                <td>NaN</td>
                <td>2</td>
                </tr>
                <tr>
                <th>OperationalLimitType</th>
                <th>9</th>
                <th>limitType</th>
                <td>NaN</td>
                <td>9</td>
                </tr>
                <tr>
                <th>Terminal</th>
                <th>144</th>
                <th>TopologicalNode</th>
                <td>NaN</td>
                <td>40</td>
                </tr>
                <tr>
                <th rowspan="7" valign="top">TopologicalNode</th>
                <th rowspan="7" valign="top">20</th>
                <th>entsoe_boundaryPoint</th>
                <td>NaN</td>
                <td>20</td>
                </tr>
                <tr>
                <th>entsoe_fromEndIsoCode</th>
                <td>NaN</td>
                <td>20</td>
                </tr>
                <tr>
                <th>entsoe_fromEndName</th>
                <td>NaN</td>
                <td>20</td>
                </tr>
                <tr>
                <th>entsoe_fromEndNameTso</th>
                <td>NaN</td>
                <td>20</td>
                </tr>
                <tr>
                <th>entsoe_toEndIsoCode</th>
                <td>NaN</td>
                <td>20</td>
                </tr>
                <tr>
                <th>entsoe_toEndName</th>
                <td>NaN</td>
                <td>20</td>
                </tr>
                <tr>
                <th>entsoe_toEndNameTso</th>
                <td>NaN</td>
                <td>20</td>
                </tr>
            </tbody>
        </table>
        <br>
    </embed>

********************
Note
********************
As of version 0.6, the linter does not yet validate many-to-many relationships.
