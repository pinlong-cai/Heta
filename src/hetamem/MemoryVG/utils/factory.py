import importlib
from typing import Dict, Optional, Union

from MemoryVG.configs.embeddings.base import BaseEmbedderConfig
from MemoryVG.configs.llms.anthropic import AnthropicConfig
from MemoryVG.configs.llms.azure import AzureOpenAIConfig
from MemoryVG.configs.llms.base import BaseLlmConfig
from MemoryVG.configs.llms.deepseek import DeepSeekConfig
from MemoryVG.configs.llms.lmstudio import LMStudioConfig
from MemoryVG.configs.llms.ollama import OllamaConfig
from MemoryVG.configs.llms.openai import OpenAIConfig
from MemoryVG.configs.llms.vllm import VllmConfig
from MemoryVG.embeddings.mock import MockEmbeddings


def load_class(class_type):
    module_path, class_name = class_type.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)


class LlmFactory:
    """
    Factory for creating LLM instances with appropriate configurations.
    Supports both old-style BaseLlmConfig and new provider-specific configs.
    """

    # Provider mappings with their config classes
    provider_to_class = {
        "ollama": ("MemoryVG.llms.ollama.OllamaLLM", OllamaConfig),
        "openai": ("MemoryVG.llms.openai.OpenAILLM", OpenAIConfig),
        "groq": ("MemoryVG.llms.groq.GroqLLM", BaseLlmConfig),
        "together": ("MemoryVG.llms.together.TogetherLLM", BaseLlmConfig),
        "aws_bedrock": ("MemoryVG.llms.aws_bedrock.AWSBedrockLLM", BaseLlmConfig),
        "litellm": ("MemoryVG.llms.litellm.LiteLLM", BaseLlmConfig),
        "azure_openai": ("MemoryVG.llms.azure_openai.AzureOpenAILLM", AzureOpenAIConfig),
        "openai_structured": (
            "MemoryVG.llms.openai_structured.OpenAIStructuredLLM",
            OpenAIConfig,
        ),
        "anthropic": ("MemoryVG.llms.anthropic.AnthropicLLM", AnthropicConfig),
        "azure_openai_structured": (
            "MemoryVG.llms.azure_openai_structured.AzureOpenAIStructuredLLM",
            AzureOpenAIConfig,
        ),
        "gemini": ("MemoryVG.llms.gemini.GeminiLLM", BaseLlmConfig),
        "deepseek": ("MemoryVG.llms.deepseek.DeepSeekLLM", DeepSeekConfig),
        "xai": ("MemoryVG.llms.xai.XAILLM", BaseLlmConfig),
        "sarvam": ("MemoryVG.llms.sarvam.SarvamLLM", BaseLlmConfig),
        "lmstudio": ("MemoryVG.llms.lmstudio.LMStudioLLM", LMStudioConfig),
        "vllm": ("MemoryVG.llms.vllm.VllmLLM", VllmConfig),
        "langchain": ("MemoryVG.llms.langchain.LangchainLLM", BaseLlmConfig),
    }

    @classmethod
    def create(
        cls,
        provider_name: str,
        config: Optional[Union[BaseLlmConfig, Dict]] = None,
        **kwargs,
    ):
        """
        Create an LLM instance with the appropriate configuration.

        Args:
            provider_name (str): The provider name (e.g., 'openai', 'anthropic')
            config: Configuration object or dict. If None, will create default config
            **kwargs: Additional configuration parameters

        Returns:
            Configured LLM instance

        Raises:
            ValueError: If provider is not supported
        """
        if provider_name not in cls.provider_to_class:
            raise ValueError(f"Unsupported Llm provider: {provider_name}")

        class_type, config_class = cls.provider_to_class[provider_name]
        llm_class = load_class(class_type)

        # Handle configuration
        if config is None:
            # Create default config with kwargs
            config = config_class(**kwargs)
        elif isinstance(config, dict):
            # Merge dict config with kwargs
            config.update(kwargs)
            config = config_class(**config)
        elif isinstance(config, BaseLlmConfig):
            # Convert base config to provider-specific config if needed
            if config_class != BaseLlmConfig:
                # Convert to provider-specific config
                config_dict = {
                    "model": config.model,
                    "temperature": config.temperature,
                    "api_key": config.api_key,
                    "max_tokens": config.max_tokens,
                    "top_p": config.top_p,
                    "top_k": config.top_k,
                    "enable_vision": config.enable_vision,
                    "vision_details": config.vision_details,
                    "http_client_proxies": config.http_client,
                }
                config_dict.update(kwargs)
                config = config_class(**config_dict)
            else:
                # Use base config as-is
                pass
        else:
            # Assume it's already the correct config type
            pass

        return llm_class(config)

    @classmethod
    def register_provider(cls, name: str, class_path: str, config_class=None):
        """
        Register a new provider.

        Args:
            name (str): Provider name
            class_path (str): Full path to LLM class
            config_class: Configuration class for the provider (defaults to BaseLlmConfig)
        """
        if config_class is None:
            config_class = BaseLlmConfig
        cls.provider_to_class[name] = (class_path, config_class)

    @classmethod
    def get_supported_providers(cls) -> list:
        """
        Get list of supported providers.

        Returns:
            list: List of supported provider names
        """
        return list(cls.provider_to_class.keys())


class EmbedderFactory:
    provider_to_class = {
        "openai": "MemoryVG.embeddings.openai.OpenAIEmbedding",
        "ollama": "MemoryVG.embeddings.ollama.OllamaEmbedding",
        "huggingface": "MemoryVG.embeddings.huggingface.HuggingFaceEmbedding",
        "azure_openai": "MemoryVG.embeddings.azure_openai.AzureOpenAIEmbedding",
        "gemini": "MemoryVG.embeddings.gemini.GoogleGenAIEmbedding",
        "vertexai": "MemoryVG.embeddings.vertexai.VertexAIEmbedding",
        "together": "MemoryVG.embeddings.together.TogetherEmbedding",
        "lmstudio": "MemoryVG.embeddings.lmstudio.LMStudioEmbedding",
        "langchain": "MemoryVG.embeddings.langchain.LangchainEmbedding",
        "aws_bedrock": "MemoryVG.embeddings.aws_bedrock.AWSBedrockEmbedding",
    }

    @classmethod
    def create(cls, provider_name, config, vector_config: Optional[dict]):
        if (
            provider_name == "upstash_vector"
            and vector_config
            and vector_config.enable_embeddings
        ):
            return MockEmbeddings()
        class_type = cls.provider_to_class.get(provider_name)
        if class_type:
            embedder_instance = load_class(class_type)
            base_config = BaseEmbedderConfig(**config)
            return embedder_instance(base_config)
        else:
            raise ValueError(f"Unsupported Embedder provider: {provider_name}")


class VectorStoreFactory:
    provider_to_class = {
        "qdrant": "MemoryVG.vector_stores.qdrant.Qdrant",
        "chroma": "MemoryVG.vector_stores.chroma.ChromaDB",
        "pgvector": "MemoryVG.vector_stores.pgvector.PGVector",
        "milvus": "MemoryVG.vector_stores.milvus.MilvusDB",
        "upstash_vector": "MemoryVG.vector_stores.upstash_vector.UpstashVector",
        "azure_ai_search": "MemoryVG.vector_stores.azure_ai_search.AzureAISearch",
        "pinecone": "MemoryVG.vector_stores.pinecone.PineconeDB",
        "mongodb": "MemoryVG.vector_stores.mongodb.MongoDB",
        "redis": "MemoryVG.vector_stores.redis.RedisDB",
        "valkey": "MemoryVG.vector_stores.valkey.ValkeyDB",
        "databricks": "MemoryVG.vector_stores.databricks.Databricks",
        "elasticsearch": "MemoryVG.vector_stores.elasticsearch.ElasticsearchDB",
        "vertex_ai_vector_search": "MemoryVG.vector_stores.vertex_ai_vector_search.GoogleMatchingEngine",
        "opensearch": "MemoryVG.vector_stores.opensearch.OpenSearchDB",
        "supabase": "MemoryVG.vector_stores.supabase.Supabase",
        "weaviate": "MemoryVG.vector_stores.weaviate.Weaviate",
        "faiss": "MemoryVG.vector_stores.faiss.FAISS",
        "langchain": "MemoryVG.vector_stores.langchain.Langchain",
        "s3_vectors": "MemoryVG.vector_stores.s3_vectors.S3Vectors",
        "baidu": "MemoryVG.vector_stores.baidu.BaiduDB",
    }

    @classmethod
    def create(cls, provider_name, config):
        class_type = cls.provider_to_class.get(provider_name)
        if class_type:
            if not isinstance(config, dict):
                config = config.model_dump()
            vector_store_instance = load_class(class_type)
            return vector_store_instance(**config)
        else:
            raise ValueError(f"Unsupported VectorStore provider: {provider_name}")

    @classmethod
    def reset(cls, instance):
        instance.reset()
        return instance


class GraphStoreFactory:
    """
    Factory for creating MemoryGraph instances for different graph store providers.
    Usage: GraphStoreFactory.create(provider_name, config)
    """

    provider_to_class = {
        "memgraph": "MemoryVG.memory.memgraph_memory.MemoryGraph",
        "neptune": "MemoryVG.graphs.neptune.main.MemoryGraph",
        "kuzu": "MemoryVG.memory.kuzu_memory.MemoryGraph",
        "default": "MemoryVG.memory.graph_memory.MemoryGraph",
    }

    @classmethod
    def create(cls, provider_name, config):
        class_type = cls.provider_to_class.get(
            provider_name, cls.provider_to_class["default"]
        )
        try:
            GraphClass = load_class(class_type)
        except (ImportError, AttributeError) as e:
            raise ImportError(
                f"Could not import MemoryGraph for provider '{provider_name}': {e}"
            )
        return GraphClass(config)
