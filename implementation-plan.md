# Implementation Plan

## Layer phases

| Phase | Layer | Branch (optional) |
|-------|-------|-------------------|
| 0 | Env + scaffold | main |
| 1 | data_generation | cursor/layer-1-data-gen |
| 2 | bronze | cursor/layer-2-bronze |
| 3 | silver | cursor/layer-3-silver |
| 4 | gold | cursor/layer-4-gold |
| 5 | dashboard | cursor/layer-5-dashboard |
| 6 | Submission docs | cursor/submission |

Each layer: Superpowers brainstorm → implement → test → layer-completion → ai-prompts entry.
