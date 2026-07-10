# Repository Instructions

## Public-by-default standard

This repository is open source. Treat every committed file, line of code, test fixture, generated artifact, commit message, and pull-request description as immediately visible to the public.

## Product source of truth

`nozzle-prd.md` is the sole product source of truth. Keep requirements, architecture decisions, risks, rejected alternatives, proof obligations, and release criteria in that file. Supporting documentation may explain or link to the PRD but must not create a competing product contract.

- Never commit secrets, credentials, tokens, private URLs, personal data, Cloudflare account details, or machine-specific authentication files.
- Do not commit local state, debug logs, caches, dependency directories, temporary files, or generated output unless it is intentionally part of the public project.
- Use fictional, clearly non-sensitive values in examples and fixtures.
- Keep implementation, tests, documentation, comments, and generated files production-quality and suitable for external review.
- Keep the product and implementation stupidly simple. Prefer the smallest correct design and the fewest moving parts.
- Be smart about efficiency where measurements or platform limits justify it. Complex algorithms are acceptable only when they materially improve a real constraint, remain isolated behind a simple interface, and include benchmarks, invariants, documentation, and differential tests against a straightforward reference implementation.
- Do not introduce cleverness, abstraction, dependencies, or operational states without a concrete benefit that outweighs their maintenance and failure cost.
- Before staging or committing, inspect the complete diff and staged file list for sensitive or accidental content.
- If there is any doubt about whether information is safe to publish, stop and ask before committing it.
