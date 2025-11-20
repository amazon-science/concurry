# Documentation Standards: The "Textbook" Quality

This document establishes the standards for Concurry's documentation. Our goal is to create documentation that is not just informative but **delightful**, **empowering**, and **anticipatory**.

## Core Tenets

### 1. The "Zero-Friction" Principle
*   **Philosophy**: Documentation should minimize cognitive load. The user should never have to guess import paths, variable types, or configuration options.
*   **Implementation**:
    *   **Self-Contained Examples**: Every code snippet must be runnable. All imports must be included.
    *   **Explicit Typing**: All code examples must use Python type hints.
    *   **Contextual Defaults**: When introducing a parameter, immediately mention its default value (referencing `global_config` keys).

### 2. The "Problem-First" Narrative (Diataxis Framework)
*   **Philosophy**: Users come with problems, not a desire to read API specs. Start every section with the *problem* it solves, then present the *solution*.
*   **Implementation**:
    *   **Structure**: Problem → Naive Solution (and why it fails) → Concurry Solution (and why it works).
    *   **Empathy**: Acknowledge common frustrations (e.g., "Debugging race conditions is painful...").

### 3. The "Textbook" Depth
*   **Philosophy**: Go beyond "how" to "why". Explain underlying mechanics to build mental models. A user who understands *how* it works can solve their own edge cases.
*   **Implementation**:
    *   **Mental Models**: Use analogies (e.g., "Workers are like dedicated employees...").
    *   **Diagrams/Flows**: Use text-based diagrams to visualize data flow (e.g., User → Queue → Worker).
    *   **"Under the Hood" Blocks**: Use `!!! note "Under the Hood"` or similar for deep dives.

### 4. Anticipatory Guidance
*   **Philosophy**: Answer the question the user *is about to ask*. Predict pitfalls before they happen.
*   **Implementation**:
    *   **Pro Tips**: Highlight best practices immediately using callouts.
    *   **Common Pitfalls**: Explicit "What could go wrong" sections.
    *   **Decision Matrices**: Tables helping users choose between options (e.g., Thread vs. Process vs. Ray).

### 5. Consistent & Delightful Tone
*   **Philosophy**: The tone should be professional yet welcoming—like a senior engineer mentoring a junior colleague.
*   **Implementation**:
    *   **Active Voice**: "You define..." instead of "It is defined...".
    *   **Encouragement**: "This pattern makes your code robust..."
    *   **Clarity**: Short sentences. Bullet points over walls of text.

## Style Guide

### Code Blocks
*   Always include language identifier: ````python`
*   Use `print()` statements to show expected output.
*   Include comments explaining *why*, not just *what*.

### Headers
*   **H1 (#)**: Document Title (One per file)
*   **H2 (##)**: Major Sections
*   **H3 (###)**: Subsections
*   **H4 (####)**: Detailed topics (Avoid going deeper than H4)

### Admonitions (Callouts)
Use MkDocs admonitions for emphasis:

```markdown
!!! tip "Pro Tip"
    Use `mode="asyncio"` for I/O-bound tasks to maximize throughput.

!!! warning "Common Pitfall"
    Avoid sharing mutable state between process workers.
```

### Cross-Referencing
*   Link to other documentation sections frequently.
*   Use descriptive link text: "See [Worker Pools](pools.md) for scaling details" instead of "Click [here](pools.md)".

---
*This document is a living standard. Update it as we discover better ways to teach and delight our users.*

