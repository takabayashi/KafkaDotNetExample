# KafkaDotNetExample

# Custom Serializer Example

This project demonstrates how to use a custom serializer to replace the original Newtonsoft.Json serializer while keeping the schema registry validation active.

## Intent

The intent of this program is to exemplify the usage of a custom serializer to replace the original Newtonsoft.Json serializer but keep the schema registry validation active.

## Prerequisites

- .NET SDK installed on your machine.
- Visual Studio Code or any other IDE of your choice.

## Getting Started

1. **Clone the repository:**

    ```sh
    git clone https://github.com/takabayashi/KafkaDotNetExample.git
    cd KafkaDotNetExample
    ```

2. **Build the project:**

    ```sh
    dotnet build
    ```

3. **Run the project:**

    ```sh
    dotnet run
    ```

## Usage

This project includes a custom deserializer that uses `System.Text.Json` instead of `Newtonsoft.Json`. The deserializer ensures that the schema registry validation remains active.
