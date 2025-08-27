-- =========================================================
-- View Analítica Consolidada - BPC
-- =========================================================
CREATE OR REPLACE VIEW bpc.gold.vw_gold_bpc_analitico AS
WITH
-- ========================
-- Base Geral (macro Brasil)
-- ========================
geral AS (
    SELECT
        Beneficios_Administrativos,
        Beneficios_Judiciais,
        Prazo_Medio_Administrativo,
        Prazo_Medio_Judicial,
        Total_Beneficios,
        Percentual_Judicializacao,
        Populacao_Total AS Populacao_Total_Brasil,
        Cobertura_BPC_percent AS Cobertura_Brasil
    FROM bpc.gold.tb_gold_fato_bpc_geral
),

-- ========================
-- Base por UF
-- ========================
uf AS (
    SELECT
        u.UF,
        u.Nome_UF,
        u.Regiao,
        f.Beneficios_Administrativos,
        f.Beneficios_Judiciais,
        f.Prazo_Medio_Administrativo,
        f.Prazo_Medio_Judicial,
        f.Total_Beneficios,
        f.Percentual_Judicializacao,
        f.Populacao_Total,
        f.Cobertura_BPC_percent
    FROM bpc.gold.tb_gold_fato_bpc_uf f
    LEFT JOIN bpc.gold.tb_gold_dim_uf_regiao u
        ON f.UF = u.UF
),

-- ========================
-- Público-Alvo (População)
-- ========================
pop AS (
    SELECT
        UF,
        Populacao_Total,
        Populacao_Idosa,
        Populacao_PCD,
        Populacao_Alvo_BPC
    FROM bpc.gold.tb_gold_fato_bpc_populacao
)

-- ========================
-- Unificação
-- ========================
SELECT
    u.UF,
    u.Nome_UF,
    u.Regiao,

    -- Indicadores por UF
    u.Beneficios_Administrativos,
    u.Beneficios_Judiciais,
    u.Total_Beneficios,
    ROUND(u.Percentual_Judicializacao, 2) AS Perc_Judicializacao_UF,
    ROUND(u.Cobertura_BPC_percent, 2) AS Cobertura_UF,
    ROUND(u.Prazo_Medio_Administrativo, 1) AS Prazo_Medio_Administrativo,
    ROUND(u.Prazo_Medio_Judicial, 1) AS Prazo_Medio_Judicial,

    -- População alvo
    p.Populacao_Total,
    p.Populacao_Idosa,
    p.Populacao_PCD,
    p.Populacao_Alvo_BPC,

    -- Indicadores gerais Brasil (para comparação no dash)
    g.Beneficios_Administrativos AS Beneficios_Adm_Brasil,
    g.Beneficios_Judiciais AS Beneficios_Jud_Brasil,
    g.Total_Beneficios AS Total_Brasil,
    ROUND(g.Percentual_Judicializacao, 2) AS Perc_Judicializacao_Brasil,
    ROUND(g.Cobertura_Brasil, 2) AS Cobertura_Brasil
FROM uf u
LEFT JOIN pop p ON u.UF = p.UF
CROSS JOIN geral g;
