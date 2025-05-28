import { NextRequest, NextResponse } from "next/server";
import { z } from "zod";
import { createClient } from "redis";

// Define the data structures for the knowledge graph
const EntitySchema = z.object({
  name: z.string(),
  entityType: z.string(),
  observations: z.array(z.string()),
});

const RelationSchema = z.object({
  from: z.string(),
  to: z.string(),
  relationType: z.string(),
});

const KnowledgeGraphSchema = z.object({
  entities: z.array(EntitySchema),
  relations: z.array(RelationSchema),
});

type Entity = z.infer<typeof EntitySchema>;
type Relation = z.infer<typeof RelationSchema>;
type KnowledgeGraph = z.infer<typeof KnowledgeGraphSchema>;

// MCP Protocol Types
interface MCPRequest {
  jsonrpc: "2.0";
  id: string | number;
  method: string;
  params?: any;
}

interface MCPResponse {
  jsonrpc: "2.0";
  id: string | number;
  result?: any;
  error?: {
    code: number;
    message: string;
    data?: any;
  };
}

interface MCPToolCall {
  name: string;
  arguments: any;
}

// Redis-based Knowledge Graph Manager
class RedisKnowledgeGraphManager {
  private redis: ReturnType<typeof createClient>;
  private memoryId: string;

  constructor(redisUrl: string, memoryId: string) {
    this.redis = createClient({ url: redisUrl });
    this.memoryId = memoryId;
  }

  private async ensureConnected() {
    if (!this.redis.isOpen) {
      await this.redis.connect();
    }
  }

  private getEntityKey() {
    return `memory:${this.memoryId}:entities`;
  }

  private getRelationKey() {
    return `memory:${this.memoryId}:relations`;
  }

  private async loadGraph(): Promise<KnowledgeGraph> {
    await this.ensureConnected();
    
    const [entitiesData, relationsData] = await Promise.all([
      this.redis.get(this.getEntityKey()),
      this.redis.get(this.getRelationKey()),
    ]);

    const entities = entitiesData ? JSON.parse(entitiesData) : [];
    const relations = relationsData ? JSON.parse(relationsData) : [];

    return { entities, relations };
  }

  private async saveGraph(graph: KnowledgeGraph): Promise<void> {
    await this.ensureConnected();
    
    await Promise.all([
      this.redis.set(this.getEntityKey(), JSON.stringify(graph.entities)),
      this.redis.set(this.getRelationKey(), JSON.stringify(graph.relations)),
    ]);
  }

  async createEntities(entities: Entity[]): Promise<Entity[]> {
    const graph = await this.loadGraph();
    const newEntities = entities.filter(e => 
      !graph.entities.some(existingEntity => existingEntity.name === e.name)
    );
    graph.entities.push(...newEntities);
    await this.saveGraph(graph);
    return newEntities;
  }

  async createRelations(relations: Relation[]): Promise<Relation[]> {
    const graph = await this.loadGraph();
    const newRelations = relations.filter(r => 
      !graph.relations.some(existingRelation => 
        existingRelation.from === r.from && 
        existingRelation.to === r.to && 
        existingRelation.relationType === r.relationType
      )
    );
    graph.relations.push(...newRelations);
    await this.saveGraph(graph);
    return newRelations;
  }

  async addObservations(observations: { entityName: string; contents: string[] }[]): Promise<{ entityName: string; addedObservations: string[] }[]> {
    const graph = await this.loadGraph();
    const results = observations.map(o => {
      const entity = graph.entities.find(e => e.name === o.entityName);
      if (!entity) {
        throw new Error(`Entity with name ${o.entityName} not found`);
      }
      const newObservations = o.contents.filter(content => !entity.observations.includes(content));
      entity.observations.push(...newObservations);
      return { entityName: o.entityName, addedObservations: newObservations };
    });
    await this.saveGraph(graph);
    return results;
  }

  async deleteEntities(entityNames: string[]): Promise<void> {
    const graph = await this.loadGraph();
    graph.entities = graph.entities.filter(e => !entityNames.includes(e.name));
    graph.relations = graph.relations.filter(r => 
      !entityNames.includes(r.from) && !entityNames.includes(r.to)
    );
    await this.saveGraph(graph);
  }

  async deleteObservations(deletions: { entityName: string; observations: string[] }[]): Promise<void> {
    const graph = await this.loadGraph();
    deletions.forEach(d => {
      const entity = graph.entities.find(e => e.name === d.entityName);
      if (entity) {
        entity.observations = entity.observations.filter(o => !d.observations.includes(o));
      }
    });
    await this.saveGraph(graph);
  }

  async deleteRelations(relations: Relation[]): Promise<void> {
    const graph = await this.loadGraph();
    graph.relations = graph.relations.filter(r => 
      !relations.some(delRelation => 
        r.from === delRelation.from && 
        r.to === delRelation.to && 
        r.relationType === delRelation.relationType
      )
    );
    await this.saveGraph(graph);
  }

  async readGraph(): Promise<KnowledgeGraph> {
    return this.loadGraph();
  }

  async searchNodes(query: string): Promise<KnowledgeGraph> {
    const graph = await this.loadGraph();
    
    const filteredEntities = graph.entities.filter(e => 
      e.name.toLowerCase().includes(query.toLowerCase()) ||
      e.entityType.toLowerCase().includes(query.toLowerCase()) ||
      e.observations.some(o => o.toLowerCase().includes(query.toLowerCase()))
    );

    const filteredEntityNames = new Set(filteredEntities.map(e => e.name));
    const filteredRelations = graph.relations.filter(r => 
      filteredEntityNames.has(r.from) && filteredEntityNames.has(r.to)
    );

    return {
      entities: filteredEntities,
      relations: filteredRelations,
    };
  }

  async openNodes(names: string[]): Promise<KnowledgeGraph> {
    const graph = await this.loadGraph();
    
    const filteredEntities = graph.entities.filter(e => names.includes(e.name));
    const filteredEntityNames = new Set(filteredEntities.map(e => e.name));
    const filteredRelations = graph.relations.filter(r => 
      filteredEntityNames.has(r.from) && filteredEntityNames.has(r.to)
    );

    return {
      entities: filteredEntities,
      relations: filteredRelations,
    };
  }

  async disconnect(): Promise<void> {
    if (this.redis.isOpen) {
      await this.redis.disconnect();
    }
  }
}

// MCP Server Implementation
class MCPServer {
  private knowledgeGraphManager: RedisKnowledgeGraphManager;

  constructor(redisUrl: string, memoryId: string) {
    this.knowledgeGraphManager = new RedisKnowledgeGraphManager(redisUrl, memoryId);
  }

  async handleRequest(request: MCPRequest): Promise<MCPResponse> {
    try {
      switch (request.method) {
        case "initialize":
          return {
            jsonrpc: "2.0",
            id: request.id,
            result: {
              protocolVersion: "2024-11-05",
              capabilities: {
                tools: {},
              },
              serverInfo: {
                name: "memory-knowledge-graph-server",
                version: "1.0.0",
              },
            },
          };

        case "tools/list":
          return {
            jsonrpc: "2.0",
            id: request.id,
            result: {
              tools: [
                {
                  name: "create_entities",
                  description: "Create multiple new entities in the knowledge graph",
                  inputSchema: {
                    type: "object",
                    properties: {
                      entities: {
                        type: "array",
                        items: {
                          type: "object",
                          properties: {
                            name: { type: "string" },
                            entityType: { type: "string" },
                            observations: { type: "array", items: { type: "string" } },
                          },
                          required: ["name", "entityType", "observations"],
                        },
                      },
                    },
                    required: ["entities"],
                  },
                },
                {
                  name: "create_relations",
                  description: "Create multiple new relations between entities in the knowledge graph",
                  inputSchema: {
                    type: "object",
                    properties: {
                      relations: {
                        type: "array",
                        items: {
                          type: "object",
                          properties: {
                            from: { type: "string" },
                            to: { type: "string" },
                            relationType: { type: "string" },
                          },
                          required: ["from", "to", "relationType"],
                        },
                      },
                    },
                    required: ["relations"],
                  },
                },
                {
                  name: "add_observations",
                  description: "Add new observations to existing entities in the knowledge graph",
                  inputSchema: {
                    type: "object",
                    properties: {
                      observations: {
                        type: "array",
                        items: {
                          type: "object",
                          properties: {
                            entityName: { type: "string" },
                            contents: { type: "array", items: { type: "string" } },
                          },
                          required: ["entityName", "contents"],
                        },
                      },
                    },
                    required: ["observations"],
                  },
                },
                {
                  name: "delete_entities",
                  description: "Delete multiple entities and their associated relations from the knowledge graph",
                  inputSchema: {
                    type: "object",
                    properties: {
                      entityNames: { type: "array", items: { type: "string" } },
                    },
                    required: ["entityNames"],
                  },
                },
                {
                  name: "delete_observations",
                  description: "Delete specific observations from entities in the knowledge graph",
                  inputSchema: {
                    type: "object",
                    properties: {
                      deletions: {
                        type: "array",
                        items: {
                          type: "object",
                          properties: {
                            entityName: { type: "string" },
                            observations: { type: "array", items: { type: "string" } },
                          },
                          required: ["entityName", "observations"],
                        },
                      },
                    },
                    required: ["deletions"],
                  },
                },
                {
                  name: "delete_relations",
                  description: "Delete multiple relations from the knowledge graph",
                  inputSchema: {
                    type: "object",
                    properties: {
                      relations: {
                        type: "array",
                        items: {
                          type: "object",
                          properties: {
                            from: { type: "string" },
                            to: { type: "string" },
                            relationType: { type: "string" },
                          },
                          required: ["from", "to", "relationType"],
                        },
                      },
                    },
                    required: ["relations"],
                  },
                },
                {
                  name: "read_graph",
                  description: "Read the entire knowledge graph",
                  inputSchema: {
                    type: "object",
                    properties: {},
                  },
                },
                {
                  name: "search_nodes",
                  description: "Search for nodes in the knowledge graph based on a query",
                  inputSchema: {
                    type: "object",
                    properties: {
                      query: { type: "string" },
                    },
                    required: ["query"],
                  },
                },
                {
                  name: "open_nodes",
                  description: "Open specific nodes in the knowledge graph by their names",
                  inputSchema: {
                    type: "object",
                    properties: {
                      names: { type: "array", items: { type: "string" } },
                    },
                    required: ["names"],
                  },
                },
              ],
            },
          };

        case "tools/call":
          return await this.handleToolCall(request);

        default:
          return {
            jsonrpc: "2.0",
            id: request.id,
            error: {
              code: -32601,
              message: "Method not found",
            },
          };
      }
    } catch (error) {
      return {
        jsonrpc: "2.0",
        id: request.id,
        error: {
          code: -32603,
          message: "Internal error",
          data: error instanceof Error ? error.message : String(error),
        },
      };
    }
  }

  private async handleToolCall(request: MCPRequest): Promise<MCPResponse> {
    const { name, arguments: args } = request.params;

    try {
      let result: any;

      switch (name) {
        case "create_entities":
          result = await this.knowledgeGraphManager.createEntities(args.entities);
          break;

        case "create_relations":
          result = await this.knowledgeGraphManager.createRelations(args.relations);
          break;

        case "add_observations":
          result = await this.knowledgeGraphManager.addObservations(args.observations);
          break;

        case "delete_entities":
          await this.knowledgeGraphManager.deleteEntities(args.entityNames);
          result = "Entities deleted successfully";
          break;

        case "delete_observations":
          await this.knowledgeGraphManager.deleteObservations(args.deletions);
          result = "Observations deleted successfully";
          break;

        case "delete_relations":
          await this.knowledgeGraphManager.deleteRelations(args.relations);
          result = "Relations deleted successfully";
          break;

        case "read_graph":
          result = await this.knowledgeGraphManager.readGraph();
          break;

        case "search_nodes":
          result = await this.knowledgeGraphManager.searchNodes(args.query);
          break;

        case "open_nodes":
          result = await this.knowledgeGraphManager.openNodes(args.names);
          break;

        default:
          return {
            jsonrpc: "2.0",
            id: request.id,
            error: {
              code: -32601,
              message: `Unknown tool: ${name}`,
            },
          };
      }

      return {
        jsonrpc: "2.0",
        id: request.id,
        result: {
          content: [
            {
              type: "text",
              text: typeof result === "string" ? result : JSON.stringify(result, null, 2),
            },
          ],
        },
      };
    } catch (error) {
      return {
        jsonrpc: "2.0",
        id: request.id,
        error: {
          code: -32603,
          message: "Tool execution failed",
          data: error instanceof Error ? error.message : String(error),
        },
      };
    }
  }

  async cleanup(): Promise<void> {
    await this.knowledgeGraphManager.disconnect();
  }
}

// SSE Connection Manager
class SSEConnectionManager {
  private connections = new Map<string, ReadableStreamDefaultController>();
  private redis: ReturnType<typeof createClient>;

  constructor(redisUrl: string) {
    this.redis = createClient({ url: redisUrl });
  }

  async initialize(): Promise<void> {
    if (!this.redis.isOpen) {
      await this.redis.connect();
    }
  }

  addConnection(sessionId: string, controller: ReadableStreamDefaultController): void {
    this.connections.set(sessionId, controller);
  }

  removeConnection(sessionId: string): void {
    this.connections.delete(sessionId);
  }

  sendMessage(sessionId: string, message: any): void {
    const controller = this.connections.get(sessionId);
    if (controller) {
      const data = `data: ${JSON.stringify(message)}\n\n`;
      controller.enqueue(new TextEncoder().encode(data));
    }
  }

  async cleanup(): Promise<void> {
    if (this.redis.isOpen) {
      await this.redis.disconnect();
    }
  }
}

// Global SSE manager instance
let sseManager: SSEConnectionManager | null = null;

// Helper function to get memory ID from URL
function getMemoryIdFromParams(params: { id: string }): string {
  return params.id;
}

// HTTP POST handler for MCP requests
export async function POST(
  request: NextRequest,
  { params }: { params: { transport: string; id: string } }
): Promise<NextResponse> {
  const redisUrl = process.env.REDIS_URL;
  if (!redisUrl) {
    return NextResponse.json(
      { error: "REDIS_URL environment variable is required" },
      { status: 500 }
    );
  }

  const memoryId = getMemoryIdFromParams(params);
  const mcpServer = new MCPServer(redisUrl, memoryId);

  try {
    const body = await request.json();
    const response = await mcpServer.handleRequest(body);
    return NextResponse.json(response);
  } catch (error) {
    return NextResponse.json(
      {
        jsonrpc: "2.0",
        id: null,
        error: {
          code: -32700,
          message: "Parse error",
          data: error instanceof Error ? error.message : String(error),
        },
      },
      { status: 400 }
    );
  } finally {
    await mcpServer.cleanup();
  }
}

// SSE GET handler for real-time communication
export async function GET(
  request: NextRequest,
  { params }: { params: { transport: string; id: string } }
): Promise<Response> {
  const redisUrl = process.env.REDIS_URL;
  if (!redisUrl) {
    return new Response("REDIS_URL environment variable is required", {
      status: 500,
    });
  }

  const memoryId = getMemoryIdFromParams(params);
  const sessionId = request.nextUrl.searchParams.get("session") || `session-${Date.now()}`;

  // Initialize SSE manager if not already done
  if (!sseManager) {
    sseManager = new SSEConnectionManager(redisUrl);
    await sseManager.initialize();
  }

  const stream = new ReadableStream({
    start(controller) {
      // Add connection to manager
      sseManager!.addConnection(sessionId, controller);

      // Send initial connection message
      const initialMessage = {
        type: "connection",
        sessionId,
        memoryId,
        timestamp: new Date().toISOString(),
      };
      
      const data = `data: ${JSON.stringify(initialMessage)}\n\n`;
      controller.enqueue(new TextEncoder().encode(data));

      // Send keep-alive every 30 seconds
      const keepAlive = setInterval(() => {
        try {
          controller.enqueue(new TextEncoder().encode("data: {\"type\":\"ping\"}\n\n"));
        } catch (error) {
          clearInterval(keepAlive);
        }
      }, 30000);

      // Handle connection cleanup
      request.signal.addEventListener("abort", () => {
        clearInterval(keepAlive);
        sseManager!.removeConnection(sessionId);
        try {
          controller.close();
        } catch (error) {
          // Connection already closed
        }
      });
    },
  });

  return new Response(stream, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      "Connection": "keep-alive",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
    },
  });
}

// OPTIONS handler for CORS
export async function OPTIONS(): Promise<Response> {
  return new Response(null, {
    status: 200,
    headers: {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
    },
  });
}
