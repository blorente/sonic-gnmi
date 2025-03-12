# JSONTEST helper

## Purpose

Preparing JSON description for verification of received messages
is difficult and time consuming.

This tool tries to help with this tedious task.

## Usage

In general this tool reads JSON-encoded gNMI response and produces
JSON-encoded definition of the object to be checked by the jsontest
framework.

It is possible to use it in a number of ways.

### Scenario 1

```
$ go run jtest-gen -in gnmi-msg.json -out test.json
```

This command will read the `gnmi-msg.json` file and write generated
output to the `test.json` file.

### Scenario 2

```
$ go run jtest-gen -in gnmi-msg.json
```

This command will read the `gnmi-msg.json` file and write generated
output to the standard output (console).

### Scenario 3

```
$ cat gnmi-msg.json | go run jtest-gen -out test.json
```

This command will read the message from the standard input and
write generated output to the `test.json` file.

## Example

### Input file
```
{
    "openconfig-platform:component": [
        {
            "google-pins-platform:fpga": {
                "reset-causes": {
                    "reset-cause": [
                        {
                            "index": 1,
                            "state": {
                                "cause": "POWER",
                                "index": 1
                            }
                        }
                    ]
                }
            },
            "name": "fpga_0",
            "state": {
                "description": "fpga_0 description",
                "firmware-version": "1.0",
                "mfg-name": "GOOGLE",
                "name": "fpga_0",
                "type": "google-pins-platform:FPGA"
            }
        }
    ]
}
```

### Output file

```
{
  "type": "object",
  "required": [
    "openconfig-platform:component"
  ],
  "properties": {
    "openconfig-platform:component": {
      "openconfig-platform:component": {
        "type": "array",
        "items": [
          {
            "type": "object",
            "required": [
              "google-pins-platform:fpga",
              "name",
              "state"
            ],
            "properties": {
              "google-pins-platform:fpga": {
                "type": "object",
                "required": [
                  "reset-causes"
                ],
                "properties": {
                  "reset-causes": {
                    "type": "object",
                    "required": [
                      "reset-cause"
                    ],
                    "properties": {
                      "reset-cause": {
                        "reset-cause": {
                          "type": "array",
                          "items": [
                            {
                              "type": "object",
                              "required": [
                                "index",
                                "state"
                              ],
                              "properties": {
                                "index": {
                                  "type": "integer",
                                  "enum": [
                                    1
                                  ]
                                },
                                "state": {
                                  "type": "object",
                                  "required": [
                                    "index",
                                    "cause"
                                  ],
                                  "properties": {
                                    "index": {
                                      "type": "integer",
                                      "enum": [
                                        1
                                      ]
                                    },
                                    "cause": {
                                      "type": "string",
                                      "enum": [
                                        "POWER"
                                      ]
                                    }
                                  }
                                }
                              }
                            }
                          ]
                        }
                      }
                    }
                  }
                }
              },
              "name": {
                "type": "string",
                "enum": [
                  "fpga_0"
                ]
              },
              "state": {
                "type": "object",
                "required": [
                  "description",
                  "firmware-version",
                  "mfg-name",
                  "name",
                  "type"
                ],
                "properties": {
                  "description": {
                    "type": "string",
                    "enum": [
                      "fpga_0 description"
                    ]
                  },
                  "firmware-version": {
                    "type": "string",
                    "enum": [
                      "1.0"
                    ]
                  },
                  "mfg-name": {
                    "type": "string",
                    "enum": [
                      "GOOGLE"
                    ]
                  },
                  "name": {
                    "type": "string",
                    "enum": [
                      "fpga_0"
                    ]
                  },
                  "type": {
                    "type": "string",
                    "enum": [
                      "google-pins-platform:FPGA"
                    ]
                  }
                }
              }
            }
          }
        ]
      }
    }
  }
}
```