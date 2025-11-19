# Contributing to Tesser

First off, thank you for considering contributing to Tesser! We are thrilled to have you here. Your contributions are the lifeblood of our open-source project and are essential for its success.

This guide is designed to make your contribution process as smooth as possible, whether you're fixing a typo, adding a new feature, or improving our documentation. Every contribution is valuable, and we appreciate your effort to help us build a world-class trading framework.

## Code of Conduct

To ensure a welcoming and inclusive environment for everyone, we have adopted a Code of Conduct that all contributors are expected to follow. Please take a moment to read our **[Code of Conduct](./CODE_OF_CONDUCT.md)** before participating. We are committed to providing a friendly, safe, and positive experience for all.

## How Can I Contribute?

There are many ways to contribute to Tesser, and not all of them involve writing code. Here are a few ideas to get you started:

### 🐛 Reporting Bugs
If you find a bug, please create a detailed bug report. A great bug report includes:
- A clear and descriptive title.
- Steps to reproduce the issue.
- The expected behavior and what actually happened.
- Your environment details (OS, Rust version, etc.).

### ✨ Suggesting Enhancements
Have an idea for a new feature or an improvement to an existing one? We'd love to hear it! Please open a feature request issue and describe your idea.

### 📖 Improving Documentation
Clear documentation is crucial. You can contribute by:
- Fixing typos or grammatical errors.
- Improving the clarity of the `README.md` or other documentation.
- Adding code examples or usage tutorials.
- Commenting on complex sections of the code.

### 💻 Writing Code
If you're ready to write some code, that's fantastic! Here’s how you can find something to work on:

- **Good First Issues:** We maintain a list of issues tagged with **[`good first issue`](https://github.com/tesserspace/tesser/labels/good%20first%20issue)**. These are well-defined, smaller tasks that are perfect for new contributors to get familiar with our codebase.
- **Help Wanted:** For more challenging tasks, look for issues tagged with **[`help wanted`](https://github.com/tesserspace/tesser/labels/help%20wanted)**. These are features or fixes we'd love community help with.
- **Roadmap Items:** Check out our **[Project Roadmap](./ROADMAP.md)** for our long-term vision. If a particular item excites you, open an issue to discuss how you can contribute to it.

## Your First Code Contribution: The Workflow

Ready to submit a change? Here’s a step-by-step guide to our development process.

### 1. Fork & Clone the Repository
First, fork the repository to your own GitHub account, then clone it to your local machine:

```sh
git clone https://github.com/your-username/tesser.git
cd tesser
git remote add upstream https://github.com/tesserspace/tesser.git
```

### 2. Set Up Your Development Environment
Tesser is built with the latest stable Rust toolchain. Ensure you have `rustup` installed.

```sh
# Install or update to the latest stable Rust toolchain
rustup update stable
```

### 3. Create a New Branch
Create a branch for your changes. Please use a descriptive name, like `feat/add-atr-indicator` or `fix/backtester-pnl-calculation`.

```sh
git checkout -b your-branch-name
```

### 4. Make Your Changes
Now, write your code! As you work, please adhere to our **[Coding Style Guide](./CODING_STYLE.md)**.

A few key principles:
- Write clean, maintainable, and well-documented code.
- Add or update unit tests to cover your changes.
- Ensure all new code is formatted and passes our linter checks.

### 5. Run Local Checks
Before submitting your code, please run our full suite of local checks to ensure everything is in order.

```sh
# Format all code in the workspace
cargo fmt --all

# Run Clippy to catch common mistakes and style issues
cargo clippy --all-targets --all-features -- -D warnings

# Run all tests in release mode
cargo test --release --all
```

All checks must pass before a pull request can be merged.

### 6. Commit and Push
Commit your changes with a descriptive message that follows the **[Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/)** specification.

```sh
# Example commit message for a new feature
git commit -m "feat(indicators): Add Average True Range (ATR) indicator"

# Example for a bug fix
git commit -m "fix(portfolio): Correctly calculate PnL for short positions"
```

Keep your branch up-to-date with the `main` branch of the upstream repository:
```sh
git fetch upstream
git rebase upstream/main
```

Then, push your branch to your fork:
```sh
git push origin your-branch-name
```

### 7. Submit a Pull Request
Go to the Tesser repository on GitHub and open a Pull Request.
- Fill out the PR template with a clear description of your changes.
- Link to any relevant issues (e.g., `Closes #123`).
- If your PR is not yet ready for review, please create it as a **Draft Pull Request**.

### 8. Code Review
A maintainer will review your pull request. We may suggest some changes or improvements. We see code review as a collaborative process to improve the quality of the project, so please be open to feedback! Once your PR is approved, a maintainer will merge it.

Congratulations, and thank you for your contribution!

## Project Architecture Guide

To make a more significant contribution, it helps to understand the project's structure. Please read the **Architecture & Crate Responsibilities** section in our main **[README.md](./README.md)** for a detailed overview.

Here are some common contribution areas and where to find the relevant code:

-   **Adding a New Indicator:**
    -   **Location:** `tesser-indicators/src/indicators/`
    -   **Process:** Implement the `Indicator` trait, add comprehensive unit tests, and export it in the `mod.rs` file.

-   **Adding a New Exchange Connector:**
    -   **Location:** Create a new crate under the `connectors/` directory (e.g., `connectors/tesser-binance`).
    -   **Process:** Implement the `ExecutionClient` and `MarketStream` traits from `tesser-broker`. Use `tesser-bybit` as a reference.

-   **Adding a New Strategy:**
    -   **Location:** `tesser-strategy/src/lib.rs`
    -   **Process:** Implement the `Strategy` trait and register it using the `register_strategy!` macro.
    -   **Note:** We are actively working on a **WASM-based plugin system** to allow for dynamic strategy loading. If you're interested in systems design, this is a great area to contribute! See our [Roadmap](./ROADMAP.md) for details.

## Join Our Community

The best way to get help, discuss ideas, and connect with other contributors is to join our community channels:

-   **Discord:** [Link to your Discord server]
-   **Telegram:** [Link to your Telegram group]

Don't be shy—we're here to help you get started!

Thank you again for your interest in Tesser. We can't wait to see what you'll build with us.
