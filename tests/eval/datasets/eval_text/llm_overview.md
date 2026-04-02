# Large Language Models: An Overview

Source: arXiv:2303.18223 — "A Survey of Large Language Models"
Authors: Wayne Xin Zhao et al. (22 authors), Renmin University of China
Note: Ongoing work; 144 pages, 1081 citations as of latest version.

## Background

Language is essentially a complex, intricate system of human expressions governed
by grammatical rules. It poses a significant challenge to develop capable AI
algorithms for comprehending and grasping a language. As a major approach,
language modeling has been widely studied for language understanding and generation
in the past two decades, evolving from statistical language models to neural
language models.

Recently, pre-trained language models (PLMs) have been proposed by pre-training
Transformer models over large-scale corpora, showing strong capabilities in
solving various NLP tasks. Since researchers have found that model scaling can
lead to performance improvement, they further study the scaling effect by
increasing the model size to an even larger size. Interestingly, when the
parameter scale exceeds a certain level, these enlarged language models not only
achieve a significant performance improvement but also show some special abilities
(referred to as **emergent abilities**) that are not present in small-scale
language models. To discriminate the difference in parameter scale, the research
community has coined the term **large language models (LLM)** for the PLMs of
significant size.

## Key Model Statistics

The following statistics highlight the scale of modern language models:

- **BERT-Large**: 340 million parameters (Google, 2018). Training BERT-Large on
  GPU has a carbon footprint approximately equivalent to a trans-American flight.
- **GPT-3**: 175 billion parameters (OpenAI, 2020). Demonstrated few-shot
  learning across a wide range of NLP tasks.
- **Megatron-Turing NLG**: 530 billion parameters. Introduced jointly by
  Microsoft and NVIDIA, described at the time as "the world's largest and most
  powerful generative language model."
- **Switch Transformer**: 1.6 trillion parameters (Google, 2021). Used sparse
  mixture-of-experts routing to achieve scale without proportional compute cost.
- **PaLM**: 540 billion parameters (Google, 2022). Demonstrated strong
  chain-of-thought reasoning capabilities.

## Efficient Variants

Compression and distillation techniques have produced smaller but capable models:

- **DistilBERT**: 40% smaller and 60% faster than BERT-Large, while retaining
  97% of its language understanding capability on the GLUE benchmark.
- **Big Science T0**: 16 times smaller than GPT-3 while outperforming it on many
  multi-task benchmarks.

## Training Infrastructure

Training frontier LLMs requires significant hardware investment:

- A DGX A100 server (8× A100 GPUs) costs approximately $199,000 per unit.
- Total training infrastructure for frontier LLMs can reach approximately
  $100 million.
- LLM parameter counts have historically increased approximately 10x per year.

## Emergent Abilities

Large language models exhibit abilities that are not present in smaller models
and appear relatively suddenly at certain scale thresholds:

1. **In-context learning**: The ability to perform tasks given only a few
   examples in the prompt, without gradient updates.
2. **Instruction following**: Responding correctly to natural language
   instructions without task-specific fine-tuning.
3. **Chain-of-thought reasoning**: Generating intermediate reasoning steps to
   solve complex multi-step problems.
4. **Code generation**: Writing functional programs from natural language
   specifications.

## ChatGPT and Recent Advances

A remarkable recent progress is the launch of ChatGPT, which has attracted
widespread attention from society. ChatGPT demonstrated that instruction-tuned
and RLHF-trained LLMs could provide helpful, harmless, and honest responses at
scale. The success of ChatGPT has significantly accelerated both academic research
and commercial deployment of large language models globally.

## Summary of Survey

The survey arXiv:2303.18223 covers four key aspects of LLMs:
1. Pre-training data, architecture, and training techniques
2. Alignment tuning (instruction tuning and RLHF)
3. Utilization via prompting and in-context learning
4. Capacity evaluation across benchmarks and tasks
