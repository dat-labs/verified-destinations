## verified-destinations

This repository contains verified data destinations that you can use as a starting point for your project. It's a package designed to house DAT (Data Activation Tool) verified destinations.

### Local Development Setup

To run this project locally, follow these steps:

#### 1. Set up Python Environment

Make sure you have Python 3.10 installed on your system. If not, you can download and install it from [Python's official website](https://www.python.org/downloads/).

#### 2. Clone the Repository

```bash
git clone https://github.com/dat-labs/verified-destinations.git
```

#### 3. Create a Virtual Environment
Navigate into the cloned directory and create a virtual environment using your preferred method. If you're using venv, you can create a virtual environment like this:

```bash
cd verified-destinations
python -m venv .venv
```

#### 4. Activate the Virtual Environment
Activate the virtual environment. The command to activate it depends on your operating system.

- On Windows:
    ```bash
    .venv\Scripts\activate
    ```
- On Unix or MacOS:
    ```bash
    source .venv/bin/activate
    ```

#### 5. Install Dependencies

```bash
pip install poetry
poetry install
```

This will install all the necessary dependencies for the project.

Contributing
If you want to contribute to this project, please read the [contribution guidelines](CONTRIBUTING).

License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

Contact
For any inquiries or issues regarding the project, feel free to contact us at <team@datlabs.com>.